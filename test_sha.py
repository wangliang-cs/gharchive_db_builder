import utils.gharchive_gzreader as gg

if __name__ == "__main__":
    test_str = "github:waydetse/waydetse.github.com_github:waydetse_PushEvent_2012-08-31T17:57:27-07:00"

    # 生成SHA-256哈希（推荐）
    sha256_hash = gg.generate_sha_hash(test_str)
    print(f"SHA-256: {sha256_hash}")
    time_str = "2012-08-31T17:57:27-07:00"
    print(time_str.split('-')[0])
    print(time_str.split('-')[1])
