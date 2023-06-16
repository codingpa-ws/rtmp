package rtmp

type SessionGuard interface {
	Check(*Session) bool
	End(*Session)
}
