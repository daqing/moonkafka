// Whitebox tests for the share-group acknowledgement action codes. The
// four actions travel to the broker as the numbers the protocol defines
// (ShareAcknowledge v0: 1 Accept, 2 Release, 3 Reject, 4 Renew), so each
// variant is pinned here.

///|
test "share ack types map to the protocol action codes" {
  let cases : Array[(ShareAckType, Int)] = [
    (ShareAccept, 1),
    (ShareRelease, 2),
    (ShareReject, 3),
    (ShareRenew, 4),
  ]
  for (action, expected) in cases {
    assert_eq(action.code(), expected)
  }
}
