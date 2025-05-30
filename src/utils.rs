// B-tree validator for "left <= key < right" semantics

use crate::{
    PageId,
    cache::Cache,
    page::{Delta, Page, PageBuffer},
};

pub fn validate_tree_structure(
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
    root_id: PageId,
) -> Result<(), String> {
    validate_node(buf_pool, cache, root_id, None, None, false)
}

fn validate_node(
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
    node_id: PageId,
    min_bound: Option<&[u8]>, // Keys must be > min_bound (exclusive)
    max_bound: Option<&[u8]>, // Keys must be <= max_bound (inclusive)
    min_exclusive: bool,      // Whether min_bound is exclusive
) -> Result<(), String> {
    let node_idx = cache
        .get(node_id)
        .ok_or_else(|| format!("Node {} not in cache", node_id))?;

    let mut page = buf_pool[node_idx].read();

    if page.is_inner() {
        validate_inner_node(
            &mut page,
            buf_pool,
            cache,
            node_id,
            min_bound,
            max_bound,
            min_exclusive,
        )
    } else {
        validate_leaf_node(&mut page, node_id, min_bound, max_bound, min_exclusive)
    }
}

fn validate_inner_node(
    page: &mut Page,
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
    node_id: PageId,
    min_bound: Option<&[u8]>,
    max_bound: Option<&[u8]>,
    min_exclusive: bool,
) -> Result<(), String> {
    // Collect all effective key->child mappings after applying deltas
    let mut key_child_pairs = Vec::new();

    // First, get base page entries
    for (key, child_id) in page.base.iter_entries_inner() {
        key_child_pairs.push((key.to_vec(), child_id));
    }

    // Apply split deltas
    while let Some(delta) = page.deltas.next() {
        match delta {
            Delta::Split(split_delta) => {
                key_child_pairs.push((split_delta.middle_key.to_vec(), split_delta.left_pid));
            }
            _ => return Err(format!("Non-split delta in inner node {}", node_id)),
        }
    }

    // Sort by key
    key_child_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    // Validate key ordering within node
    for i in 1..key_child_pairs.len() {
        if key_child_pairs[i - 1].0 >= key_child_pairs[i].0 {
            return Err(format!(
                "Key ordering violation in node {}: {:?} >= {:?}",
                node_id,
                &key_child_pairs[i - 1].0[..4.min(key_child_pairs[i - 1].0.len())],
                &key_child_pairs[i].0[..4.min(key_child_pairs[i].0.len())]
            ));
        }
    }

    // Validate this node's keys against bounds
    for (key, _) in &key_child_pairs {
        if let Some(min) = min_bound {
            if min_exclusive {
                if key.as_slice() <= min {
                    return Err(format!(
                        "Key {:?} in node {} violates min bound {:?} (should be > min)",
                        &key.as_slice()[..4.min(key.len())],
                        node_id,
                        &min[..4.min(min.len())]
                    ));
                }
            } else {
                if key.as_slice() < min {
                    return Err(format!(
                        "Key {:?} in node {} violates min bound {:?} (should be >= min)",
                        &key.as_slice()[..4.min(key.len())],
                        node_id,
                        &min[..4.min(min.len())]
                    ));
                }
            }
        }

        if let Some(max) = max_bound {
            if key.as_slice() > max {
                return Err(format!(
                    "Key {:?} in node {} violates max bound {:?} (should be <= max)",
                    &key.as_slice()[..4.min(key.len())],
                    node_id,
                    &max[..4.min(max.len())]
                ));
            }
        }
    }

    // Recursively validate children
    // For your B-tree semantics: left child has keys <= boundary_key, right child has keys > boundary_key

    let mut current_min = min_bound;
    let mut current_min_exclusive = min_exclusive;

    for (boundary_key, left_child_id) in &key_child_pairs {
        // Left child: keys <= boundary_key
        validate_node(
            buf_pool,
            cache,
            *left_child_id,
            current_min,
            Some(boundary_key.as_slice()),
            current_min_exclusive,
        )?;

        // Next child will have keys > this boundary_key
        current_min = Some(boundary_key.as_slice());
        current_min_exclusive = true;
    }

    // Validate rightmost child: keys > last_boundary_key
    validate_node(
        buf_pool,
        cache,
        page.base.right_pid,
        current_min,
        max_bound,
        current_min_exclusive,
    )?;

    Ok(())
}

fn validate_leaf_node(
    page: &mut Page,
    node_id: PageId,
    min_bound: Option<&[u8]>,
    max_bound: Option<&[u8]>,
    min_exclusive: bool,
) -> Result<(), String> {
    let mut all_keys = Vec::new();

    // Collect keys from base page
    for (key, _) in page.base.iter_entries_leaf() {
        all_keys.push(key.to_vec());
    }

    // Collect keys from deltas (most recent values win)
    let mut delta_keys = std::collections::HashMap::new();
    while let Some(delta) = page.deltas.next() {
        match delta {
            Delta::Set(set_delta) => {
                delta_keys.insert(set_delta.key.to_vec(), ());
            }
            _ => return Err(format!("Non-set delta in leaf node {}", node_id)),
        }
    }

    // Add delta keys to all_keys
    for (key, _) in delta_keys {
        all_keys.push(key);
    }

    // Remove duplicates and sort
    all_keys.sort();
    all_keys.dedup();

    // Validate bounds
    for key in &all_keys {
        if let Some(min) = min_bound {
            if min_exclusive {
                if key.as_slice() <= min {
                    return Err(format!(
                        "Key {:?} in leaf {} violates min bound {:?} (should be > min)",
                        &key.as_slice()[..4.min(key.len())],
                        node_id,
                        &min[..4.min(min.len())]
                    ));
                }
            } else {
                if key.as_slice() < min {
                    return Err(format!(
                        "Key {:?} in leaf {} violates min bound {:?} (should be >= min)",
                        &key.as_slice()[..4.min(key.len())],
                        node_id,
                        &min[..4.min(min.len())]
                    ));
                }
            }
        }

        if let Some(max) = max_bound {
            if key.as_slice() > max {
                return Err(format!(
                    "Key {:?} in leaf {} violates max bound {:?} (should be <= max)",
                    &key.as_slice()[..4.min(key.len())],
                    node_id,
                    &max[..4.min(max.len())]
                ));
            }
        }
    }

    Ok(())
}
