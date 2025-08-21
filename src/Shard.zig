const std = @import("std");
const mem = std.mem;

const PageCache = @import("PageCache.zig");

const Self = @This();
page_cache: PageCache,
page_store: PageStore,

pub fn init(cfg: Config, allocator: mem.Allocator) Self {
    const page_cache = PageCache.init(
        cfg.page_size,
        cfg.pool_size,
        cfg.free_cap_target,
        allocator,
    );
    const page_store = PageStore.init();

    return Self{
        .page_cache = page_cache,
        .page_store = page_store,
    };
}

pub const Config = struct {
    page_size: usize,
    pool_size: usize,
    free_cap_target: usize,
    block_cap: usize,

    block_size: usize,
    num_block_bufs: usize,
    net_buf_size: usize,
    num_net_bufs: usize,

    queue_depth: usize,
};

pub const PageStore = struct {
    const _Self = @This();

    offset_table: std.HashMapUnmanaged(u64, u64),
    root: u64,
    next_pid: u64,

    pub fn init() _Self {
        return _Self{};
    }

    pub fn readBlock(
        _: *_Self,
        _: []const u8,
        _: u64,
        _: *PageCache,
    ) void {}
};
