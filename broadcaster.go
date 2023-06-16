package rtmp

import "fmt"

// A subscriber gets sent audio, video and data messages that flow in a particular stream (identified with streamKey)
type Subscriber interface {
	SendAudio(audio []byte, timestamp uint32)
	SendVideo(video []byte, timestamp uint32)
	SendMetadata(metadata map[string]any)
	GetID() string
	SendEndOfStream()
}

type Broadcaster interface {
	BroadcastAudio(streamKey string, audio []byte, timestamp uint32) error
	BroadcastEndOfStream(streamKey string)
	BroadcastMetadata(streamKey string, metadata map[string]any) error
	BroadcastVideo(streamKey string, video []byte, timestamp uint32) error
	DestroyPublisher(streamKey string) error
	DestroySubscriber(streamKey string, sessionID string) error
	GetAacSequenceHeaderForPublisher(streamKey string) []byte
	GetAvcSequenceHeaderForPublisher(streamKey string) []byte
	RegisterPublisher(streamKey string) error
	RegisterSubscriber(streamKey string, subscriber Subscriber) error
	SetAacSequenceHeaderForPublisher(streamKey string, payload []byte)
	SetAvcSequenceHeaderForPublisher(streamKey string, payload []byte)
	StreamExists(streamKey string) bool
	SetSessionGuard(SessionGuard)
	GetSessionGuard() SessionGuard
	AppName() string
}

type broadcaster struct {
	appName      string
	context      ContextStore
	sessionGuard SessionGuard
}

func NewBroadcaster(appName string, context ContextStore) Broadcaster {
	return &broadcaster{
		appName: appName,
		context: context,
	}
}

func (b *broadcaster) RegisterPublisher(streamKey string) error {
	return b.context.RegisterPublisher(streamKey)
}

func (b *broadcaster) DestroyPublisher(streamKey string) error {
	return b.context.DestroyPublisher(streamKey)
}

func (b *broadcaster) RegisterSubscriber(streamKey string, subscriber Subscriber) error {
	return b.context.RegisterSubscriber(streamKey, subscriber)
}

func (b *broadcaster) StreamExists(streamKey string) bool {
	return b.context.StreamExists(streamKey)
}

func (b *broadcaster) BroadcastAudio(streamKey string, audio []byte, timestamp uint32) error {
	subscribers, err := b.context.GetSubscribersForStream(streamKey)
	if err != nil {
		fmt.Println("broadcaster: BroadcastAudio: error getting subscribers for stream, " + err.Error())
		return err
	}
	for _, sub := range subscribers {
		sub.SendAudio(audio, timestamp)
	}
	return nil
}

func (b *broadcaster) BroadcastVideo(streamKey string, video []byte, timestamp uint32) error {
	subscribers, err := b.context.GetSubscribersForStream(streamKey)
	if err != nil {
		fmt.Println("broadcaster: BroadcastVideo: error getting subscribers for stream, " + err.Error())
		return err
	}

	for _, sub := range subscribers {
		sub.SendVideo(video, timestamp)
	}
	return nil
}

func (b *broadcaster) DestroySubscriber(streamKey string, sessionID string) error {
	return b.context.DestroySubscriber(streamKey, sessionID)
}

func (b *broadcaster) SetAvcSequenceHeaderForPublisher(streamKey string, payload []byte) {
	b.context.SetAvcSequenceHeaderForPublisher(streamKey, payload)
}

func (b *broadcaster) GetAvcSequenceHeaderForPublisher(streamKey string) []byte {
	return b.context.GetAvcSequenceHeaderForPublisher(streamKey)
}

func (b *broadcaster) SetAacSequenceHeaderForPublisher(streamKey string, payload []byte) {
	b.context.SetAacSequenceHeaderForPublisher(streamKey, payload)
}

func (b *broadcaster) GetAacSequenceHeaderForPublisher(streamKey string) []byte {
	return b.context.GetAacSequenceHeaderForPublisher(streamKey)
}

func (b *broadcaster) BroadcastEndOfStream(streamKey string) {
	subscribers, err := b.context.GetSubscribersForStream(streamKey)
	if err != nil {
		fmt.Println("broadcaster: broadcast end of stream: error getting subscribers for stream, " + err.Error())
		return
	}

	for _, sub := range subscribers {
		sub.SendEndOfStream()
	}
}

func (b *broadcaster) BroadcastMetadata(streamKey string, metadata map[string]any) error {
	subscribers, err := b.context.GetSubscribersForStream(streamKey)
	if err != nil {
		fmt.Println("broadcaster: BroadcastVideo: error getting subscribers for stream, " + err.Error())
		return err
	}

	for _, sub := range subscribers {
		sub.SendMetadata(metadata)
	}
	return nil
}

func (b *broadcaster) SetSessionGuard(guard SessionGuard) {
	b.sessionGuard = guard
}

func (b *broadcaster) GetSessionGuard() SessionGuard {
	return b.sessionGuard
}

func (b *broadcaster) AppName() string {
	return b.appName
}
