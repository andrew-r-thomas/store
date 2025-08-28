const std = @import("std");

const mem = std.mem;
const heap = std.heap;

const debug = std.debug;
const print = debug.print;

const math = std.math;

const format = @import("format.zig");
const Shard = @import("Shard.zig");

const Self = @This();

state: union(State) {
    zipping: struct {
        request: ZipRequest,
    },
    idle,
},
zip_prios: std.PriorityQueue(
    ZipRequest,
    ZipRequest.Ctx,
    ZipRequest.compare,
),
zip_arena: heap.ArenaAllocator,

pub fn init(allocator: mem.Allocator) Self {
    return Self{
        .state = .idle,
        .zip_prios = .init(allocator, .{}),
        .zip_arena = .init(allocator),
    };
}

/// ## TODO
/// - make this an actual state machine: .building_parent, .building_children, etc
pub fn pump(
    self: *Self,
    block_server: *Shard.BlockServer,
    levels: *std.ArrayListUnmanaged(Shard.LevelMeta),
    safe_ts: u64,
) !void {
    const alloc = self.zip_arena.allocator();
    switch (self.state) {
        .zipping => |_| {},
        .idle => {
            const req = self.zip_prios.removeOrNull() orelse return;

            // build parent
            var active_commits = std.ArrayListUnmanaged(format.Commit).empty;
            var deduped_commits = std.AutoArrayHashMapUnmanaged(
                []const u8,
                format.Commit,
            ).empty;
            var smops = std.ArrayListUnmanaged(format.Smop).empty;
            const entries: format.InnerEntries = undefined;
            var parent_offset = levels.items[req.parent_level]
                .offset_table.get(req.parent_id).?;
            build_parent: while (true) {
                const block_idx, const block_start = block_server.getBlockIdx(
                    req.parent_level,
                    parent_offset,
                ) catch {
                    self.state = .{ .zipping = .{ .request = req } };
                    return;
                };
                block_server.pin(block_idx);
                const block = block_server.getBlock(block_idx);
                switch (Shard.chunkFromBlock(.inner, block, block_start)) {
                    .commits => |c| {
                        while (c.commits.next()) |commit| {
                            if (commit.timestamp <= safe_ts) {
                                const res = try deduped_commits.getOrPut(
                                    commit.write.key,
                                );
                                if (!res.found_existing) {
                                    res.val.* = commit;
                                }
                            } else {
                                try active_commits.append(alloc, commit);
                            }
                        }
                        parent_offset = c.next;
                    },
                    .smops => |s| {
                        while (s.smops.next()) |smop| {
                            try smops.append(alloc, smop);
                        }
                        parent_offset = s.next;
                    },
                    .entries => |e| {
                        entries = e;
                        break :build_parent;
                    },
                }
            }
        },
    }
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

pub const State = enum {
    zipping,
    idle,
};
