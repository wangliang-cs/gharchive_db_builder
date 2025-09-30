import utils.gharchive_gzreader as gg

if __name__ == "__main__":
    test_str = "github:alexreisner/geocoder_github:alexreisner_CreateEvent_2012-12-21T20:56:29Z"

    # 生成SHA-256哈希（推荐）
    sha256_hash = gg.generate_sha_hash(test_str)
    print(f"SHA-256: {sha256_hash}")
