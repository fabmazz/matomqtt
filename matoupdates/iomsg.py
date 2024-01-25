import msgpack
import zstandard as zstd

def save_msgpack_zstd(fpath, obj, level=10):
    try:
        cctx = zstd.ZstdCompressor(threads=-1,level=level)
        with open(fpath, "wb") as f:
            with cctx.stream_writer(f) as cmp:
                msgpack.dump(obj, cmp)

        return True
    except Exception as e:
        print(f"Cannot save file in {fpath}, error: {e}")
        return False

def read_msgpack_zstd(f):
    decomp = zstd.ZstdDecompressor()
    with open(f,mode="rb") as f:
        with decomp.stream_reader(f) as st:
            dat=msgpack.load(st)
    return dat