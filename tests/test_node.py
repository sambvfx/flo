import flo.api


def test_call():

    state = []

    def fn(arg1: int):
        state.append(arg1)

    node = flo.api.Node(fn).init(arg1=1)
    node()
    assert state == [1]
