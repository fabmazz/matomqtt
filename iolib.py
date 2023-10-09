import zstandard as zstd
import json
import io


def save_json_zstd(fpath, obj, level=10):
    cctx = zstd.ZstdCompressor(threads=-1,level=level)
    with open(fpath, "wb") as f:
        with cctx.stream_writer(f) as compressor:
            wr = io.TextIOWrapper(compressor, encoding='utf-8')
            json.dump(obj, wr)
            wr.flush()
            wr.close()

def read_json_zstd(f):
    decomp = zstd.ZstdDecompressor()
    with open(f,mode="rb") as f:
        with decomp.stream_reader(f) as st:
            dat=json.load(st)
    return dat