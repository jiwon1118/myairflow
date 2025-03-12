from myairflow.send_noti import send_noti


def test_noti():
    msg = "pytest:jiwon"
    r = send_noti(msg)
    assert r == 204
