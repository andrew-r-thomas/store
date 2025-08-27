const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const debug = std.debug;
const print = debug.print;

const format = @import("format.zig");
const Shard = @import("Shard.zig");

pub const PageBuilder = struct {
    const Self = @This();

    entries: ?format.InnerEntries,
    commits: std.ArrayListUnmanaged(format.Commit),
    smops: std.ArrayListUnmanaged(format.Smo),

    pub const empty = Self{
        .entries = null,
        .commits = std.ArrayListUnmanaged(format.Commit).empty,
        .smops = std.ArrayListUnmanaged(format.Commit).empty,
    };

    /// flush self commits down to children based on separator keys
    pub fn flush(
        self: *Self,
        allocator: mem.Allocator,
        children: []Self,
    ) !void {
        commits: for (self.commits.items) |commit| {
            for (0..self.entries.key_offs.len) |i| {
                const len = self.entries.key_lens[i];
                const off = self.entries.key_offs[i];
                const key = self.entries.keys[off .. off + len];
                switch (mem.order(u8, commit.write.key, key)) {
                    .lt, .eq => {
                        try children[i].commits.append(allocator, commit);
                        continue :commits;
                    },
                    .gt => {},
                }
            }
            try children[children.len - 1].commits.append(allocator, commit);
        }
        self.commits = std.ArrayListUnmanaged(format.Commit).empty;
    }

    /// write self out as a sequence of page chunks
    pub fn serialize(self: *const Self, buf: []u8) void {
        debug.assert(self.size() == buf.len);
        const t = format.PageChunk(.inner);
        // for now, this will always just be a chunk of commits, or an empty smo chunk
        var cursor: usize = 0;
        if (self.commits.items.len > 0) {
            buf[cursor] = @intFromEnum(t.Tag.commits);
            cursor += @sizeOf(u8);
            mem.writeInt(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                @intCast(self.commitsSize()),
                .little,
            );
            cursor += @sizeOf(u64);
            mem.writeInt(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                self.entries_off,
                .little,
            );
            cursor += @sizeOf(u64);
            for (self.commits.items) |commit| {
                commit.serialize(buf[cursor .. cursor + commit.size()]);
                cursor += commit.size();
            }
        } else {
            buf[cursor] = @intFromEnum(t.Tag.smops);
            cursor += @sizeOf(u8);
            mem.writeInt(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                0,
                .little,
            );
            cursor += @sizeOf(u64);
            mem.writeInt(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                self.entries_off,
                .little,
            );
        }
    }

    /// returns the total serialized size of self
    pub fn size(self: *const Self) usize {
        return format.PageChunk(.inner).HEADER_SIZE + self.commitsSize();
    }

    fn commitsSize(self: *const Self) usize {
        var total: usize = 0;
        for (self.commits.items) |commit| {
            total += commit.size();
        }
        return total;
    }
};

/// flow:
/// - accumulate all chunks for parent and children
/// - flush commits to children
/// - if needed, compact children
/// - if needed, split/merge children
///   - this will add smops to parent
/// - serialize children
/// - serizlize parent
pub fn zip(
    allocator: mem.Allocator,
    block_server: *Shard.BlockServer,
    top: TopLevel,
    bottom_meta: *Shard.LevelMeta,
) !void {
    var arena = heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parent_builder = parent: switch (top) {
        .root => |_| {
            // TODO
            break :parent PageBuilder.empty;
        },
        .level => |level| {
            const top_meta, const parent_id = level;
            var parent_builder = PageBuilder.empty;
            var offset = top_meta.offset_table.get(parent_id).?;

            chunks: while (true) {
                const block_idx = try block_server.getBlockIdx(
                    top_meta.level,
                    offset,
                );
                const block = block_server.getBlock(block_idx);
                const chunk = Shard.chunkFromBlock(.inner, block, offset);
                switch (chunk) {
                    .commits => |c| {
                        const commits = c.commits;
                        const next = c.next;
                        while (commits.next()) |commit| {
                            parent_builder.commits.append(alloc, commit);
                        }
                        offset = next;
                    },
                    .smops => |s| {
                        const smops = s.smops;
                        const next = s.next;
                        while (smops.next()) |smop| {
                            parent_builder.smops.append(alloc, smop);
                        }
                        offset = next;
                    },
                    .entries => |e| {
                        parent_builder.entries = e;
                        break :chunks;
                    },
                }
            }

            break :parent parent_builder;
        },
    };

    const parent_entries = switch (parent_chunks[parent_chunks.len - 1]) {
        .entries => |e| e,
        else => unreachable,
    };
    const parent_offset = if (parent_chunks.len > 1)
        switch (parent_chunks[parent_chunks.len - 2]) {
            .commits => |c| c.next,
            else => unreachable,
        }
    else
        parent_top_offset;
    var parent_builder = try PageBuilder.init(
        alloc,
        parent_entries,
        parent_offset,
        parent_chunks[0 .. parent_chunks.len - 1],
    );

    // build children
    var children = std.ArrayListUnmanaged(PageBuilder).empty;
    for (0..child_chunks.len) |i| {
        const chunks = child_chunks[i];
        const child_entries = switch (chunks[chunks.len - 1]) {
            .entries => |e| e,
            else => unreachable,
        };
        const child_offset = if (chunks.len > 1)
            switch (chunks[chunks.len - 2]) {
                .commits => |c| c.next,
                else => unreachable,
            }
        else
            child_top_offsets[i];

        try children.append(alloc, try PageBuilder.init(
            alloc,
            child_entries,
            child_offset,
            chunks[0 .. chunks.len - 1],
        ));
    }

    // flush ops down to children
    try parent_builder.flush(alloc, children.items);

    // serialize children and parent
    var child_cursor: usize = 0;
    for (children.items) |child| {
        child.serialize(child_buf[child_cursor .. child_cursor + child.size()]);
        child_cursor += child.size();
    }
    parent_builder.serialize(parent_buf[0..parent_builder.size()]);
}

pub const TopLevel = union(Tag) {
    root: *Shard.Root,
    level: struct { *Shard.LevelMeta, u64 },
    pub const Tag = enum {
        root,
        level,
    };
};
