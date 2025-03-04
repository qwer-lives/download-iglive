// download/backwards.rs

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use reqwest::{Client, Url};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinError;

use super::download_file;
use crate::error::IgLiveError;
use crate::mpd::{MediaType, Representation};
use crate::state::State;

pub async fn download_reps_backwards(
    state: Arc<Mutex<State>>,
    client: &Client,
    url_base: &Url,
    reps: impl IntoIterator<Item = (&Representation, ProgressBar)>,
    start_frame: usize,
    dir: impl AsRef<Path> + Send,
    parallel_candidates: usize,
) -> Result<()> {
    futures::future::try_join_all(reps.into_iter().map(|(rep, pb)| {
        download_backwards(state.clone(), client, url_base, rep, start_frame, dir.as_ref(), pb, parallel_candidates)
    }))
    .await?;
    Ok(())
}

async fn download_backwards(
    state: Arc<Mutex<State>>,
    client: &Client,
    url_base: &Url,
    rep: &Representation,
    start_frame: usize,
    dir: impl AsRef<Path>,
    pb: ProgressBar,
    parallel_candidates: usize,
) -> Result<()> {
    let media_type = rep.media_type();
    let mut latest_t = *state.lock().await.downloaded_segs[&media_type]
        .iter()
        .min()
        .unwrap() as isize;

    let mut visited: BTreeSet<isize> = BTreeSet::new();
    let mut pts_too_early_segments: BTreeSet<isize> = BTreeSet::new();
    let mut lower_bound = 0;
    let mut prev_delta = 0;
    let assumed_missing_delta = 2000;
    let mut skipped_segments = 0;

    let concurrency_limit = 10;
    let semaphore = Arc::new(Semaphore::new(concurrency_limit));

    pb.set_message(format!("Latest: {}", latest_t));

    while latest_t > start_frame as isize {
        let candidates =
            find_next_candidates(&state, &media_type, latest_t, &mut visited, lower_bound, &pb, parallel_candidates).await;

        if candidates.is_empty() {
            // No candidate found.  Assume a segment is missing *here*.
            pb.println(format!("Segment near {} appears to be missing, skipping.", latest_t));
            latest_t -= assumed_missing_delta;
            lower_bound = 0;
            visited.insert(latest_t);
            // Consider PTS too early segments for next round of candidates
            for &seg in &pts_too_early_segments {
                visited.remove(&seg);
            }
            pts_too_early_segments.clear();
            skipped_segments += 1;

            if skipped_segments > 5 {
                pb.println("Too many consecutive missing segments.  Giving up.");
                break;
            }
            continue;
        }

        // Create a stream of futures for the download tasks.
        let download_tasks = stream::iter(candidates)
            .map(|(candidate_t, delta)| {
                let state = state.clone();
                let client = client.clone();
                let url_base = url_base.clone();
                let rep = rep.clone();
                let dir = dir.as_ref().to_path_buf();
                let pb = pb.clone();
                let semaphore = semaphore.clone();
                let media_type = media_type.clone();

                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.expect("Semaphore error");

                    pb.set_message(format!(
                        "{:?} Latest: {} | Prev Δ: {} | Checking: {} (Δ{})",
                        media_type, latest_t, prev_delta, candidate_t, delta
                    ));
                    pb.tick();

                    let url = rep.download_url(&url_base, candidate_t)?;
                    let filename = dir.join(
                        url.path_segments()
                            .ok_or(IgLiveError::InvalidUrl)?
                            .rev()
                            .next()
                            .ok_or(IgLiveError::InvalidUrl)?,
                    );

                    let result = download_file(
                        state.clone(),
                        &client,
                        media_type,
                        (skipped_segments == 0), // ignore PTS check if we've lost previous segment(s)
                        &url,
                        filename,
                    )
                    .await;

                    Ok::<_, anyhow::Error>((candidate_t, delta, result))
                })
            })
            .buffer_unordered(concurrency_limit)
            .map(|res| match res {
                Ok(inner_result) => inner_result,
                Err(join_err) => {
                    Err(anyhow::anyhow!("Task panicked: {:?}", join_err))
                }
            });

        // Process the results of the download tasks.
        let results: Vec<Result<(isize, isize, Result<()>)>> = download_tasks.collect().await;

        let mut downloaded_any = false;
        for result in results {
            match result {
                Ok((candidate_t, delta, download_result)) => match download_result {
                    Ok(()) => {
                        prev_delta = delta;
                        latest_t = candidate_t;
                         *state.lock().await.deltas.get_mut(&media_type).unwrap().entry(delta).or_insert(0) += 1;
                        skipped_segments = 0;
                        // Consider PTS too early segments for next round of candidates
                        for &seg in &pts_too_early_segments {
                            visited.remove(&seg);
                        }
                        pts_too_early_segments.clear();
                    }
                    Err(e) => {
                        if let Some(e) = e.downcast_ref::<IgLiveError>() {
                            match e {
                                IgLiveError::StatusNotFound => (),
                                IgLiveError::PtsTooEarly => {
                                    pb.println(format!(
                                        "{media_type:?} Found but PTS too early, adjusting lower bound to {candidate_t} - offset {delta}"
                                    ));
                                    lower_bound = candidate_t;
                                    pts_too_early_segments.insert(candidate_t);
                                }
                                _ => pb.println(format!("Download failed: {e:?}")),
                            }
                        }
                    }
                },
                Err(e) => {
                    pb.println(format!("Task error: {e:?}"));
                }
            }
        }
    }

    pb.finish_with_message("Finished");
    Ok(())
}

async fn find_next_candidates(
    state: &Arc<Mutex<State>>,
    media_type: &MediaType,
    latest_t: isize,
    visited: &mut BTreeSet<isize>,
    lower_bound: isize,
    pb: &ProgressBar,
    parallel_candidates: usize,
) -> Vec<(isize, isize)> {
    let search_range = 1000;
    let mut candidates = Vec::new();

    // Get deltas sorted by count (descending)
    let locked_state = state.lock().await;
    let deltas_map = &locked_state.deltas[media_type];
    let mut deltas: Vec<_> = deltas_map.iter().collect();
    deltas.sort_by(|(_, a), (_, b)| b.cmp(a));

    for offset in 0..=search_range {
        for (&delta, cc) in &deltas {
            let potential_candidates = [latest_t - (delta + offset), latest_t - (delta - offset)];
            for &candidate_t in &potential_candidates {
                if candidate_t > lower_bound && candidate_t < latest_t && !visited.contains(&candidate_t) {
                    candidates.push((candidate_t, latest_t - candidate_t));
                    visited.insert(candidate_t);
                    if candidates.len() >= parallel_candidates {
                        return candidates;
                    }
                }
            }
        }
    }

    candidates
}
