const std = @import("std");
const debug = std.debug;
const print = debug.print;
const heap = std.heap;

const store = @import("store_lib");
const Shard = store.shard.Shard;
const Mesh = store.Mesh;

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}).init;
    var shard = try Shard.init(
        Shard.Config{
            .block_size = 1024 * 1024,
            .num_blocks = 64,
            .max_page_size = 1024,

            // NOTE: these are not currently being used internally
            .zip_cfg = .{
                .zip_thresh = 1024,
                .compact_thresh = 1024,
                .split_thresh = 1024,
                .merge_thresh = 1024,
            },
        },
        gpa.allocator(),
    );
    defer shard.deinit();

    while (true) {
        shard.pump();
    }
}
