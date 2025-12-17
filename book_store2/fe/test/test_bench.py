from fe.bench.run import run_bench


def test_bench():
    try:
        assert 200 == 200
        run_bench()
    except Exception as e:
        assert 200 == 100, "test_bench过程出现异常"
