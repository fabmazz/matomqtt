import zstandard as zstd
import json
import io
import sys
import pickle


def save_json_zstd(fpath, obj, level=10):
    try:
        cctx = zstd.ZstdCompressor(threads=-1,level=level)
        with open(fpath, "wb") as f:
            with cctx.stream_writer(f) as compressor:
                wr = io.TextIOWrapper(compressor, encoding='utf-8')
                json.dump(obj, wr)
                wr.flush()
                wr.close()
        return True
    except Exception as e:
        print(f"Cannot save file in {fpath}, error: {e}", file=sys.stderr)
        return False

def save_pickle_zstd(fpath, obj, level=10):
    try:
        cctx = zstd.ZstdCompressor(threads=-1,level=level)
        with open(fpath, "wb") as f:
            with cctx.stream_writer(f) as compressor:
                pickle.dump(obj, compressor,protocol=4)
        return True
    except Exception as e:
        print(f"Cannot save file in {fpath}, error: {e}", file=sys.stderr)
        return False

def save_npdict_zstd(fpath, dictofarrs, level=10):
    import numpy as np
    try:
        cctx = zstd.ZstdCompressor(threads=-1,level=level)
        with open(fpath, "wb") as f:
            with cctx.stream_writer(f) as compressor:
                #pickle.dump(obj, compressor,protocol=4)
                np.savez(f, **dictofarrs)
        return True
    except Exception as e:
        print(f"Cannot save file in {fpath}, error: {e}", file=sys.stderr)
        return False

def read_json_zstd(f):
    decomp = zstd.ZstdDecompressor()
    with open(f,mode="rb") as f:
        with decomp.stream_reader(f) as st:
            dat=json.load(st)
    return dat

def read_json_pickle(f):
    decomp = zstd.ZstdDecompressor()
    with open(f,mode="rb") as f:
        with decomp.stream_reader(f) as st:
            dat=pickle.load(st)
    return dat