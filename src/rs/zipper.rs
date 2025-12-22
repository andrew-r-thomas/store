use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    format::{Commit, Entries, InnerEntries, Serialize, Smop},
    io::PageId,
};

use bytes::Bytes;
use dashmap::{DashMap, Entry};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct Zipper {
    active_zips: Arc<DashMap<PageId, Arc<Notify>>>,
    config: Config,
}
impl Zipper {
    pub async fn reserve_page(&self, page_id: PageId) -> ZipGuard<'_> {
        let notify = {
            match self.active_zips.entry(page_id) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    entry.insert(Arc::new(Notify::new()));
                    return ZipGuard {
                        page_id,
                        zipper: self,
                    };
                }
            }
        };
        notify.notified().await;
        ZipGuard {
            page_id,
            zipper: self,
        }
    }

    // only doing inner-inner no merge for now
    pub fn trigger_zip(
        &self,
        parent_id: PageId,
        child_level: u8,
        oldest_active_ts: Arc<AtomicU64>,

        io_handle: IOHandle,
    ) {
        let zipper = self.clone();
        let current_oats = oldest_active_ts.load(Ordering::Acquire);
        tokio::spawn(async move {
            // build parent
            let _parent_guard = zipper.reserve_page(parent_id).await;

            let parent_commits_chain = io_handle.get_commits(parent_id);
            let parent_smops_chain = io_handle.get_smops(parent_id);
            let parent_entries_fut = io_handle.get_entries::<InnerEntries>(parent_id);

            let parent_entries = parent_entries_fut.await;

            // dump commits
            let mut children = HashMap::<u64, (usize, Vec<Commit>)>::new();
            if let Some(commits) = parent_commits {
                'commits: for commit in commits {
                    if let Some(smops) = parent_smops {
                        smops.reset();
                        for smop in smops {
                            if commit.write.key() > smop.gt_key
                                && commit.write.key() <= smop.lte_key
                            {
                                if let Some(child) = children.get_mut(&smop.pid) {
                                    child.0 += commit.size();
                                    child.1.push(commit);
                                } else {
                                    children.insert(smop.pid, (commit.size(), vec![commit]));
                                }
                                continue 'commits;
                            }
                        }
                    }

                    let pid = parent_entries.search(&commit.write.key());
                    if let Some(child) = children.get_mut(&pid) {
                        child.0 += commit.size();
                        child.1.push(commit);
                    } else {
                        children.insert(pid, (commit.size(), vec![commit]));
                    }
                }
            }

            // process children
            let mut new_parent_smops = Vec::<Smop>::new();
            for (child_pid, (new_commits_size, new_commits)) in children {
                let child_id = PageId {
                    pid: child_pid,
                    level: child_level,
                };
                let _guard = zipper.reserve_page(child_id).await;

                if child
                    .commits
                    .as_ref()
                    .map_or_else(|| 0, |commits| commits.size())
                    + new_commits_size
                    >= zipper.config.commit_compact_thresh
                {
                    // compact commits
                    let mut active_commits = Vec::new();
                    let mut deduped_commits = HashMap::new();
                    for commit in new_commits {
                        if commit.ts >= current_oats {
                            active_commits.push(commit);
                        } else {
                            deduped_commits.entry(commit.write.key()).or_insert(commit);
                        }
                    }
                    if let Some(commits) = child.commits {
                        for commit in commits {
                            if commit.ts >= current_oats {
                                active_commits.push(commit);
                            } else {
                                deduped_commits.entry(commit.write.key()).or_insert(commit);
                            }
                        }
                    }
                }
                if child.smops.as_ref().map_or_else(|| 0, |smops| smops.size())
                    >= zipper.config.smop_compact_thresh
                {
                    // compact smops
                    let mut new_entries = BTreeMap::<Bytes, u64>::new();
                    if let Some(smops) = child.smops {
                        for smop in smops {}
                    }

                    // if child.size() >= config.split_thresh {
                    //     // split
                    //     let (smop, mut new_page) = child.split();
                    //     new_smops.push(smop);
                    //     new_page.set_pid(page_server.new_page(child_level));
                    //     new_page.publish(page_server).await;
                    // }
                }

                // serialize and publish
                todo!()
            }

            // serialize and publish parent
            todo!()
        });
    }
}

pub struct ZipGuard<'g> {
    page_id: PageId,
    zipper: &'g Zipper,
}
impl Drop for ZipGuard<'_> {
    fn drop(&mut self) {
        match self.zipper.active_zips.entry(self.page_id) {
            Entry::Occupied(mut entry) => {
                let notify = entry.get_mut();
                if let Some(_) = Arc::get_mut(notify) {
                    entry.remove();
                } else {
                    notify.notify_one();
                }
            }
            _ => panic!(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Config {
    commit_compact_thresh: usize,
    smop_compact_thresh: usize,
    split_thresh: usize,
}
