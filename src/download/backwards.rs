use std::collections::{VecDeque, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures::future;
use indicatif::ProgressBar;
use reqwest::{Client, Url};
use tokio::sync::Mutex;

use super::download_file;
use crate::error::IgLiveError;
use crate::mpd::{Representation, MediaType};
use crate::state::State;


pub async fn download_reps_backwards(
    state: Arc<Mutex<State>>,
    client: &Client,
    url_base: &Url,
    reps: impl IntoIterator<Item = (&Representation, ProgressBar)>,
    start_frame: usize,
    dir: impl AsRef<Path> + Send,
) -> Result<()> {
    future::try_join_all(reps.into_iter().map(|(rep, pb)| {
        download_backwards(state.clone(), client, url_base, rep, start_frame, dir.as_ref(), pb)
    }))
    .await?;
    Ok(())
}


fn insert_closest_candidates(
    delta: isize,
    queue: &mut VecDeque<isize>,
    visited: &mut HashSet<isize>,
) {
    if !visited.contains(&delta) {
        visited.insert(delta);
        queue.push_back(delta);
    } else {
        for i in (delta+1)..(delta+6000) {
            if !visited.contains(&i) {
                visited.insert(i);
                queue.push_back(i);
                break;
            }
        }
        for i in ((delta-6000)..(delta-1)).rev() {
            if i < 0 {
                break;
            }
            if !visited.contains(&i) {
                visited.insert(i);
                queue.push_back(i);
                break;
            }
        }
    }
}


async fn generate_candidates(
    state: Arc<Mutex<State>>,
    media_type: &MediaType,
    visited: &mut HashSet<isize>,
) -> VecDeque<isize> {
    let mut queue: VecDeque<isize> = VecDeque::new();
    let deltas_count = state.lock().await.deltas[media_type].clone();
    let mut deltas_count: Vec<_> = deltas_count.into_iter().collect();
    deltas_count.sort_by(|(_, a), (_, b)| b.cmp(a));
    for (delta, _) in deltas_count {
        insert_closest_candidates(delta, &mut queue, visited);
    }
    queue
}


async fn add_new_front_candidates(
    state: Arc<Mutex<State>>,
    media_type: &MediaType,
    queue: &mut VecDeque<isize>,
    visited: &mut HashSet<isize>,
) {
    let deltas_count = state.lock().await.deltas[media_type].clone();
    let mut deltas_count: Vec<_> = deltas_count.into_iter().collect();
    deltas_count.sort_by(|(_, a), (_, b)| b.cmp(a));
    for (delta, _) in deltas_count {
        if !visited.contains(&delta) {
            visited.insert(delta);
            queue.push_front(delta);
        }
    }
}


async fn download_next_segment(
    latest_t: isize,
    state: Arc<Mutex<State>>,
    client: &Client,
    url_base: &Url,
    rep: &Representation,
    dir: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Option<(isize, isize)> {
    let media_type = rep.media_type();
    let mut visited = HashSet::new();
    let mut queue = generate_candidates(state.clone(), &media_type, &mut visited).await;
    let mut lower_bound = 0;
    let mut count = 0;
    let mut fallback_segment = -1;

    while let Some(x) = queue.pop_front() {
        let t = latest_t - x;
        if t < lower_bound {
            // Maybe add closest candidates here too?
            continue;
        }
        count += 1;
        
        pb.set_message(format!(
            "Downloaded segment {}, checking {}, delta {}, checked {}, queue size {}",
            latest_t, t, x, count, queue.len()
        ));
        pb.tick();

        let url = rep.download_url(url_base, t).ok()?;
        let filename = dir
            .as_ref()
            .join(url.path_segments()?.rev().next()?);
        match download_file(state.clone(), client, rep.media_type(), true, &url, filename).await {
            Ok(()) => return Some((t, x)),
            Err(e) => {
                if let Some(e) = e.downcast_ref::<IgLiveError>() {
                    match e {
                        IgLiveError::StatusNotFound => insert_closest_candidates(x, &mut queue, &mut visited),
                        IgLiveError::PtsTooEarly => {
                            pb.println(format!("Found but PTS too early, adjusting lower bound to {t}"));
                            lower_bound = t;
                            fallback_segment = t;
                        }
                        _ => pb.println(format!("Download failed: {e:?}")),
                    }
                }
            }
        }
        if count % 20 == 0 {
            add_new_front_candidates(state.clone(), &media_type, &mut queue, &mut visited).await;
        }
        // If ran out of elements, try fallback without checking PTS before finishing
        if queue.is_empty() && fallback_segment >= 0 {
            pb.println(format!("Trying fallback segment {fallback_segment} before giving up"));
            let url2 = rep.download_url(url_base, fallback_segment).ok()?;
            let filename2 = dir
                .as_ref()
                .join(url2.path_segments()?.rev().next()?);
        
            match download_file(state.clone(), client, rep.media_type(), false, &url2, filename2).await {
                Ok(()) => return Some((fallback_segment, x)),
                Err(e) => pb.println(format!("Download failed: {e:?}")),
            }
        }
    }
    None
}


async fn download_backwards(
    state: Arc<Mutex<State>>,
    client: &Client,
    url_base: &Url,
    rep: &Representation,
    start_frame: usize,
    dir: impl AsRef<Path>,
    pb: ProgressBar,
) -> Result<()> {
    let media_type = rep.media_type();
    let mut latest_t = *state.lock().await.downloaded_segs[&media_type]
        .iter()
        .min()
        .unwrap() as isize;
    while latest_t > start_frame as isize {
        if let Some((downloaded_segment, offset)) = download_next_segment(latest_t, state.clone(), client, url_base, rep, dir.as_ref(), &pb).await {
            latest_t = downloaded_segment;
            *state.lock().await.deltas.get_mut(&media_type).unwrap().entry(offset).or_insert(0) += 1;
        } else {
            pb.println(format!("Couldn't find previous segment, giving up at {latest_t}"));
            break;
        }
    }
    pb.finish_with_message("Finished");
    Ok(())
}

