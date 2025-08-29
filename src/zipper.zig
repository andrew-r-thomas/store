//! ## NOTES
//! - we don't ever zip with the root as the parent, the root will update the
//!   top level pages priorities when it flushes, when we choose a top level
//!   page as a parent, we also compact the parent, and let the smops cascade
//!   up to the root, since we can modify that directly in memory easily
//! - our initial state will be an empty root pointing to two empty inner
//!   pages at the top level, each pointing respectively to two empty leaf
//!   pages, this way our structure is always in the normal operational shape
//!   we also don't really waste space doing bc of our LFS style file layout

const std = @import("std");

const mem = std.mem;
const heap = std.heap;

const debug = std.debug;
const print = debug.print;

const math = std.math;

const format = @import("format.zig");
const Shard = @import("Shard.zig");

const Self = @This();

state: State,
zip_prios: std.PriorityQueue(ZipRequest, ZipRequest.Ctx, ZipRequest.compare),
zip_arena: heap.ArenaAllocator,
pins: std.ArrayListUnmanaged(usize),
cfg: Cfg,

pub fn init(allocator: mem.Allocator, cfg: Cfg) Self {
    return Self{
        .state = .idle,
        .zip_prios = .init(allocator, .{}),
        .zip_arena = .init(allocator),
        .pins = .empty,
        .cfg = cfg,
    };
}

pub fn pump(
    self: *Self,
    block_server: *Shard.BlockServer,
    levels: *std.ArrayListUnmanaged(Shard.LevelMeta),
    oldest_active_commit: u64,
) !void {
    const alloc = self.zip_arena.allocator();

    // a wild stackless coroutine has appeared!
    state: switch (self.state) {
        .idle => {
            const req = self.zip_prios.removeOrNull() orelse return;
            self.state = .{
                .building_parent = .{
                    .req = req,
                    .next = levels.items[req.parent_level].offset_table.get(req.parent_id).?,
                    .parent = PageBuilder(.inner).empty,
                },
            };
            continue :state self.state;
        },
        .building_parent => |*data| {
            const block_idx, const block_start = block_server.getBlockIdx(
                data.req.parent_level,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.parent.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.inner, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                const child_map = try data.parent.dumpCommits(alloc);
                self.state = .{
                    .zipping = .{ .req = data.req, .parent = data.parent, .child_map = child_map },
                };
                continue :state self.state;
            }
        },
        .zipping => |*data| {
            if (data.child_map.pop()) |child| {
                const child_level = data.req.parent_level - 1;
                if (child_level == 0) {
                    self.state = .{
                        .building_leaf_child = .{
                            .zipping = data.*,
                            .new_child_commits = child.value,
                            .next = levels.items[child_level].offset_table.get(child.key).?,
                            .child = .empty,
                        },
                    };
                } else {
                    self.state = .{
                        .building_inner_child = .{
                            .zipping = data.*,
                            .new_child_commits = child.value,
                            .next = levels.items[child_level].offset_table.get(child.key).?,
                            .child = .empty,
                        },
                    };
                }
                continue :state self.state;
            } else {
                if (data.req.parent_level == levels.items.len - 1) {
                    // TODO: parent is top level, run compaction/split check
                }

                const level_buffer = &levels.items[
                    data.req.parent_level
                ].current_buf;
                const buf = level_buffer.buf[level_buffer.off..];
                if (buf.len < data.parent.total_size) {
                    // TODO: get a new buffer, and flush the current one
                }
                data.parent.serialize(buf[0..data.parent.total_size]);
                level_buffer.off += data.parent.total_size;

                for (self.pins.items) |idx| {
                    block_server.unpin(idx);
                }
                debug.assert(self.zip_arena.reset(.retain_capacity));
                self.state = .idle;
                return;
            }
        },
        .building_inner_child => |*data| {
            const block_idx, const block_start = block_server.getBlockIdx(
                data.zipping.req.parent_level - 1,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.child.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.inner, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                // TODO:
                // - decide on compaction
                // - decide on split
                self.state = .{ .zipping = data.zipping };
                continue :state self.state;
            }
        },
        .building_leaf_child => |*data| {
            const child_level = data.zipping.req.parent_level - 1;
            const block_idx, const block_start = block_server.getBlockIdx(
                child_level,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.child.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.leaf, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                if (data.child.total_size > self.cfg.compact_thresh) {
                    // TODO: compact, then maybe split
                    data.child.compact(oldest_active_commit);
                }

                const level_buffer = &levels.items[child_level].current_buf;
                const buf = level_buffer.buf[level_buffer.off..];
                if (buf.len < data.child.total_size) {
                    // TODO: get a new buffer, and flush the current one
                }
                data.child.serialize(buf[0..data.child.total_size]);
                level_buffer.off += data.child.total_size;

                self.state = .{ .zipping = data.zipping };
                continue :state self.state;
            }
        },
    }
}

pub fn PageBuilder(comptime pt: format.page_type) type {
    const Entries = switch (pt) {
        .inner => format.InnerEntries,
        .leaf => format.LeafEntries,
    };

    return struct {
        commits: std.ArrayListUnmanaged(format.Commit),
        smops: std.ArrayListUnmanaged(format.Smop),
        entries: ?Entries,
        total_size: usize,

        const empty = @This(){
            .commits = .empty,
            .smops = .empty,
            .entries = null,
            .total_size = 0,
        };

        pub fn ingestChunk(
            self: *@This(),
            allocator: mem.Allocator,
            chunk: format.PageChunk(pt),
        ) !?u64 {
            self.total_size += chunk.size();
            switch (chunk) {
                .commits => |c| {
                    var commits = c.commits;
                    while (commits.next()) |commit| {
                        try self.commits.append(allocator, commit);
                    }
                    return c.next;
                },
                .smops => |s| {
                    var smops = s.smops;
                    while (smops.next()) |smop| {
                        try self.smops.append(allocator, smop);
                    }
                    return s.next;
                },
                .entries => |e| {
                    self.entries = e;
                    return null;
                },
            }
        }

        pub fn dumpCommits(
            self: *@This(),
            allocator: mem.Allocator,
        ) !std.AutoArrayHashMapUnmanaged(u64, std.ArrayListUnmanaged(format.Commit)) {
            comptime {
                debug.assert(pt == .inner);
            }

            var child_map = std.AutoArrayHashMapUnmanaged(
                u64,
                std.ArrayListUnmanaged(format.Commit),
            ).empty;
            commits: for (self.commits.items) |commit| {
                for (self.smops.items) |smop| {
                    const gt_order = mem.order(u8, commit.write.key, smop.gt_key);
                    const lte_order = mem.order(u8, commit.write.key, smop.lte_key);
                    if ((gt_order == .gt) and ((lte_order == .lt) or (lte_order == .eq))) {
                        const res = try child_map.getOrPut(allocator, smop.pid);
                        if (!res.found_existing) {
                            res.value_ptr.* = .empty;
                        }
                        try res.value_ptr.append(allocator, commit);
                        continue :commits;
                    }
                }
                const res = try child_map.getOrPut(
                    allocator,
                    self.entries.?.search(commit.write.key),
                );
                if (!res.found_existing) {
                    res.value_ptr.* = .empty;
                }
                try res.value_ptr.append(allocator, commit);
            }

            return child_map;
        }

        pub fn compact(_: *@This(), _: u64) void {}

        pub fn serialize(_: *@This(), _: []u8) void {}
    };
}

pub const ZipRequest = struct {
    parent_id: u64,
    parent_level: usize,

    commit_depth: u64,

    pub const Ctx = struct {};

    pub fn compare(_: Ctx, a: @This(), b: @This()) math.Order {
        return math.order(a.commit_depth, b.commit_depth);
    }
};

pub const State = union(Tag) {
    idle,
    building_parent: BuildingParent,
    zipping: Zipping,
    building_inner_child: BuildingChild(.inner),
    building_leaf_child: BuildingChild(.leaf),

    pub const Tag = enum {
        idle,
        building_parent,
        zipping,
        building_inner_child,
        building_leaf_child,
    };

    pub const BuildingParent = struct {
        req: ZipRequest,
        next: u64,
        parent: PageBuilder(.inner),
    };
    pub const Zipping = struct {
        req: ZipRequest,
        parent: PageBuilder(.inner),
        child_map: std.AutoArrayHashMapUnmanaged(u64, std.ArrayListUnmanaged(format.Commit)),
    };
    pub fn BuildingChild(comptime pt: format.page_type) type {
        return struct {
            zipping: Zipping,
            new_child_commits: std.ArrayListUnmanaged(format.Commit),
            next: u64,
            child: PageBuilder(pt),
        };
    }
};

pub const Cfg = struct {
    /// the maximum number of total bytes a page's commit section can contain
    /// before being requested for a zip
    zip_thresh: u64,
    /// the maximum number of total bytes a page can contain before being
    /// compacted
    compact_thresh: u64,
    /// the maximum number of total bytes a page's entries section can contain
    /// before being split
    split_thresh: u64,
    /// the minimum number of total bytes a page's entries section can contain
    /// before being merged
    merge_thresh: u64,
};
