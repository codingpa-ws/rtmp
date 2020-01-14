package rtmp

import (
	"bufio"
	"fmt"
	"github.com/torresjeff/rtmp-server/audio"
	"github.com/torresjeff/rtmp-server/config"
	"github.com/torresjeff/rtmp-server/rand"
	"github.com/torresjeff/rtmp-server/video"
	"io"
	"net"
	"strings"
)

const RtmpVersion3 = 0x03


type surroundSound struct {
	stereoSound bool
	twoPointOneSound bool
	threePointOneSound bool
	fourPointZeroSound bool
	fourPointOneSound bool
	fivePointOneSound bool
	sevenPointOneSound bool
}

type clientMetadata struct {
	duration      float64
	fileSize      float64
	width         float64
	height        float64
	videoCodecID  string
	// number representation of videoCodecID (ffmpeg sends audioCodecID as a number rather than a string (like obs))
	nVideoCodecID float64
	videoDataRate float64
	frameRate     float64
	audioCodecID  string
	// number representation of audioCodecID (ffmpeg sends audioCodecID as a number rather than a string (like obs))
	nAudioCodecID   float64
	audioDataRate   float64
	audioSampleRate float64
	audioSampleSize float64
	audioChannels   float64
	sound           surroundSound
	encoder         string
}

// Media Server interface defines the callbacks that are called when a message is received
type MediaServer interface {
	onSetChunkSize(size uint32)
	onAbortMessage(chunkStreamId uint32)
	onAck(sequenceNumber uint32)
	onSetWindowAckSize(windowAckSize uint32)
	onSetBandwidth(windowAckSize uint32, limitType uint8)
	onConnect(csID uint32, transactionId float64, data map[string]interface{})
	onReleaseStream(csID uint32, transactionId float64, args map[string]interface{}, streamKey string)
	onFCPublish(csID uint32, transactionId float64, args map[string]interface{}, streamKey string)
	onCreateStream(csID uint32, transactionId float64, data map[string]interface{})
	onPublish(csID uint32, streamID uint32, transactionId float64, args map[string]interface{}, streamKey string, publishingType string)
	onSetDataFrame(metadata map[string]interface{})
	onFCUnpublish(csID uint32, transactionId float64, args map[string]interface{}, streamKey string)
	onDeleteStream(csID uint32, transactionId float64, args map[string]interface{}, streamID float64)
	onCloseStream(csID uint32, transactionId float64, args map[string]interface{})
	onAudioMessage(format audio.Format, sampleRate audio.SampleRate, sampleSize audio.SampleSize, channels audio.Channel, bytes []byte)
	onVideoMessage(frameType video.FrameType, codec video.Codec, bytes []byte)
}

// Represents a connection made with the RTMP server where messages are exchanged between client/server.
type Session struct {
	MediaServer
	sessionID      uint32
	conn           net.Conn
	socket         *bufio.ReadWriter
	clientMetadata clientMetadata
	context        *Context // Stores session data

	//handshakeState HandshakeState
	c1 []byte
	s1 []byte

	// Interprets messages, calling the appropriate callback on the session
	messageManager *MessageManager

	// app data
	app          string
	flashVer     string
	swfUrl       string
	tcUrl        string
	typeOfStream string
	streamKey    string // used to identify user
}

func NewSession(sessionID uint32, conn *net.Conn, context *Context) *Session {
	session := &Session{
		sessionID: sessionID,
		conn:      *conn,
		socket:    bufio.NewReadWriter(bufio.NewReaderSize(*conn, config.BuffioSize), bufio.NewWriterSize(*conn, config.BuffioSize)),
		context:   context,
	}
	chunkHandler := NewChunkHandler(session.socket)
	session.messageManager = NewMessageManager(session, chunkHandler)
	context.RegisterSession(session.sessionID, session)
	return session
}

// Run performs the initial handshake and starts receiving streams of data.
func (session *Session) Run() error {
	// Perform handshake
	err := session.Handshake()
	if err != nil {
		session.context.DestroySession(session.sessionID)
		session.conn.Close()
		return err
	}

	if config.Debug {
		fmt.Println("Handshake completed successfully")
	}

	// After handshake, start reading chunks
	for {
		if err := session.messageManager.nextMessage(); err == io.EOF {
			session.context.DestroySession(session.sessionID)
			session.conn.Close()
			return nil
		} else if err != nil {
			session.context.DestroySession(session.sessionID)
			return err
		}
	}
}

func (session *Session) Handshake() error {
	var err error
	if err = session.readC0C1(); err != nil {
		return err
	}
	if err = session.sendS0S1S2(); err != nil {
		return err
	}
	if err = session.readC2(); err != nil {
		return err
	}

	return nil
}

func (session *Session) GetID() uint32 {
	return session.sessionID
}

func (session *Session) send(bytes []byte) error {
	if _, err := session.socket.Write(bytes); err != nil {
		return err
	}
	if err := session.socket.Flush(); err != nil {
		return err
	}
	return nil
}

func (session *Session) write(bytes []byte) error {
	if _, err := session.socket.Write(bytes); err != nil {
		return err
	}
	return nil
}

func (session *Session) flush() error {
	if err := session.socket.Flush(); err != nil {
		return err
	}
	return nil
}

func (session *Session) read(bytes []byte) error {
	if _, err := session.socket.Read(bytes); err != nil {
		return err
	}
	return nil
}

func (session *Session) readC0C1() error {
	var c0c1 [1537]byte
	err := session.read(c0c1[:])
	if err != nil {
		return err
	}
	// Extract c1 message (which starts at byte 1) and store it for future use (sending it in S2)
	session.c1 = c0c1[1:]
	return nil
}

func (session *Session) readC2() error {
	var c2 [1536]byte
	if err := session.read(c2[:]); err != nil {
		return err
	}
	return nil
}

// Sends the s0, s1, and s2 sequence
func (session *Session) sendS0S1S2() error {
	var s0s1s2 [1 + 2*1536]byte
	var err error
	// s0 message is stored in byte 0
	s0s1s2[0] = RtmpVersion3
	// s1 message is stored in bytes 1-1536
	if err = session.generateS1(s0s1s2[1:1537]); err != nil {
		return err
	}
	// s2 message is stored in bytes 1537-3073
	if err = session.generateS2(s0s1s2[1537:]); err != nil {
		return err
	}

	return session.send(s0s1s2[:])
}

// Generates an S1 message and stores it in s1
func (session *Session) generateS1(s1 []byte) error {
	// the s1 byte array is zero-initialized, since we didn't modify it, we're sending our time as 0
	err := rand.GenerateRandomDataFromBuffer(s1[8:])
	if err != nil {
		return err
	}
	session.s1 = s1[:]
	return nil
}


// Generates an S1 message and stores it in s2. The S2 message is an echo of the C1 message
func (session *Session) generateS2(s2 []byte) error {
	copy(s2[:], session.c1)
	return nil
}

func (session *Session) onWindowAckSizeReached(sequenceNumber uint32) {
}

func (session *Session) onConnect(csID uint32, transactionID float64, data map[string]interface{}) {
	session.storeMetadata(data)
	// If the app name to connect is App (whatever the user specifies in the config, ie. "app", "app/publish"),
	if session.app == config.App {
		// Initiate connect sequence
		// As per the specification, after the connect command, the server sends the protocol message Window Acknowledgment Size
		session.messageManager.sendWindowAckSize(config.DefaultClientWindowSize)
		// TODO: connect to the app (whatever that is)
		// After sending the window ack size message, the server sends the set peer bandwidth message
		session.messageManager.sendSetPeerBandWidth(config.DefaultClientWindowSize, LimitDynamic)
		// Send the User Control Message to begin stream with stream ID = DefaultPublishStream (which is 0)
		// Subsequent messages sent by the client will have stream ID = DefaultPublishStream, until another sendBeginStream message is sent
		session.messageManager.sendBeginStream(config.DefaultPublishStream)
		// Send Set Chunk Size message
		session.messageManager.sendSetChunkSize(config.DefaultChunkSize)
		// Send Connect Success response
		session.messageManager.sendConnectSuccess(csID)
	} else {
		fmt.Println("session: user trying to connect to app", session.app, ", but the app doesn't exist")
	}
}

func (session *Session) storeMetadata(metadata map[string]interface{}) {
	// Playback clients send other properties in the command object, such as what audio/video codecs the client supports
	session.app = metadata["app"].(string)
	if _, exists := metadata["flashVer"]; exists {
		session.flashVer = metadata["flashVer"].(string)
	} else if _, exists := metadata["flashver"]; exists {
		session.flashVer = metadata["flashver"].(string)
	}

	if _, exists := metadata["swfUrl"]; exists {
		session.flashVer = metadata["swfUrl"].(string)
	} else if _, exists := metadata["swfurl"]; exists {
		session.flashVer = metadata["swfurl"].(string)
	}

	if _, exists := metadata["tcUrl"]; exists {
		session.flashVer = metadata["tcUrl"].(string)
	} else if _, exists := metadata["tcurl"]; exists {
		session.flashVer = metadata["tcurl"].(string)
	}

	if _, exists := metadata["type"]; exists {
		session.flashVer = metadata["type"].(string)
	}
}

func (session *Session) onSetChunkSize(size uint32) {
	session.messageManager.SetChunkSize(size)
}

func (session *Session) onAbortMessage(chunkStreamId uint32) {
}

func (session *Session) onAck(sequenceNumber uint32) {
}

func (session *Session) onSetWindowAckSize(windowAckSize uint32) {
	session.messageManager.SetWindowAckSize(windowAckSize)
}

func (session *Session) onSetBandwidth(windowAckSize uint32, limitType uint8) {
	session.messageManager.SetBandwidth(windowAckSize, limitType)
}

func (session *Session) onSetDataFrame(metadata map[string]interface{}) {
	// Put all keys in lowercase to handle different formats for each client uniformly
	for key, val := range metadata {
		metadata[strings.ToLower(key)] = val
	}

	if val, exists := metadata["duration"]; exists {
		session.clientMetadata.duration = val.(float64)
	}
	if val, exists := metadata["filesize"]; exists {
		session.clientMetadata.fileSize = val.(float64)
	}
	if val, exists := metadata["width"]; exists {
		session.clientMetadata.width = val.(float64)
	}
	if val, exists := metadata["height"]; exists {
		session.clientMetadata.height = val.(float64)
	}
	if val, exists := metadata["videocodecid"]; exists {
		switch val.(type) {
		case float64:
			session.clientMetadata.nVideoCodecID = val.(float64)
		case string:
			session.clientMetadata.videoCodecID = val.(string)
		}
	}
	if val, exists := metadata["videodatarate"]; exists {
		session.clientMetadata.videoDataRate = val.(float64)
	}
	if val, exists := metadata["framerate"]; exists {
		session.clientMetadata.frameRate = val.(float64)
	}
	if val, exists := metadata["audiocodecid"]; exists {
		switch val.(type) {
		case float64:
			session.clientMetadata.nAudioCodecID = val.(float64)
		case string:
			session.clientMetadata.audioCodecID = val.(string)
		}
	}
	if val, exists := metadata["audiodatarate"]; exists {
		session.clientMetadata.audioDataRate = val.(float64)
	}
	if val, exists := metadata["audiosamplerate"]; exists {
		session.clientMetadata.audioSampleRate = val.(float64)
	}
	if val, exists := metadata["audiosamplesize"]; exists {
		session.clientMetadata.audioSampleSize = val.(float64)
	}
	if val, exists := metadata["audiochannels"]; exists {
		session.clientMetadata.audioChannels = val.(float64)
	}
	if val, exists := metadata["stereo"]; exists {
		session.clientMetadata.sound.stereoSound = val.(bool)
	}
	if val, exists := metadata["2.1"]; exists {
		session.clientMetadata.sound.twoPointOneSound = val.(bool)
	}
	if val, exists := metadata["3.1"]; exists {
		session.clientMetadata.sound.threePointOneSound = val.(bool)
	}
	if val, exists := metadata["4.0"]; exists {
		session.clientMetadata.sound.fourPointZeroSound = val.(bool)
	}
	if val, exists := metadata["4.1"]; exists {
		session.clientMetadata.sound.fourPointOneSound = val.(bool)
	}
	if val, exists := metadata["5.1"]; exists {
		session.clientMetadata.sound.fivePointOneSound = val.(bool)
	}
	if val, exists := metadata["7.1"]; exists {
		session.clientMetadata.sound.sevenPointOneSound = val.(bool)
	}
	if val, exists := metadata["encoder"]; exists {
		session.clientMetadata.encoder = val.(string)
	}

	//if config.Debug {
	//	fmt.Printf("clientMetadata %+v", session.clientMetadata)
	//}
}

func (session *Session) onReleaseStream(csID uint32, transactionID float64, args map[string]interface{}, streamKey string) {
	// TODO: what does releaseStream actually do? Does it close the stream?
	// TODO: check stuff like is stream key valid, is the user allowed to release the stream

	// TODO: implement
}

func (session *Session) onFCPublish(csID uint32, transactionID float64, args map[string]interface{}, streamKey string) {
	// TODO: check stuff like is stream key valid, is the user allowed to publish to the stream

	session.sendOnFCPublish(csID, transactionID, streamKey)
}

func (session *Session) sendOnFCPublish(csID uint32, transactionID float64, streamKey string) {
	message := generateOnFCPublishMessage(csID, transactionID, streamKey)
	session.socket.Write(message)
	session.socket.Flush()
}

func (session *Session) onCreateStream(csID uint32, transactionID float64, data map[string]interface{}) {
	// data object could be nil
	message := generateCreateStreamResponse(csID, transactionID, data)
	session.socket.Write(message)
	session.socket.Flush()

	session.messageManager.sendBeginStream(uint32(config.DefaultStreamID))
}

func (session *Session) onPublish(csID uint32, streamID uint32, transactionID float64, args map[string]interface{}, streamKey string, publishingType string) {
	// TODO: Handle things like look up the user's stream key, check if it's valid.
	// TODO: For example: twitch returns "Publishing live_user_<username>" in the description.
	// TODO: Handle things like recording into a file if publishingType = "record" or "append"
	// infoObject should have at least three properties: level, code, and description. But may contain other properties.
	infoObject := map[string]interface{}{
		"level": "status",
		"code": "NetStream.Publish.Start",
		"description": "Publishing live_user_<x>",
	}
	// TODO: the transaction ID for onStatus messages should be 0 as per the spec. But twitch sends the transaction ID that was in the request to "publish".
	// For now, reply with the same transaction ID.
	message := generateStatusMessage(transactionID, streamID, infoObject)
	session.socket.Write(message)
	session.socket.Flush()
}

func (session *Session) onFCUnpublish(csID uint32, transactionID float64, args map[string]interface{}, streamKey string) {
	session.context.DestroySession(session.sessionID)
}

func (session *Session) onDeleteStream(csID uint32, transactionId float64, args map[string]interface{}, streamID float64) {
	// streamID is the ID of the stream to destroy on the server
}
func (session *Session) onCloseStream(csID uint32, transactionId float64, args map[string]interface{}) {

}

// audioData is the full payload (it has the audio headers at the beginning of the payload), for easy forwarding
// If format == audio.AAC, audioData will contain AACPacketType at index 1
func (session *Session) onAudioMessage(format audio.Format, sampleRate audio.SampleRate, sampleSize audio.SampleSize, channels audio.Channel, audioData []byte) {
	if config.Debug {
		switch format {
		case audio.MP3:
			fmt.Print("format: mp3")
		case audio.AAC:
			fmt.Print("format: aac")
		}
		switch sampleRate {
		case audio.Rate44KHz:
			fmt.Print(", sample rate: 44Kz")
		default:
			fmt.Print(", other sample rate")
		}

		switch sampleSize {
		case audio.Size8Bit:
			fmt.Print(", 8-bit sample size")
		case audio.Size16Bit:
			fmt.Print(", 16-bit sample size")
		}

		switch channels {
		case audio.Mono:
			fmt.Print(", mono channels\n")
		case audio.Stereo:
			fmt.Print(", stereo channels\n")
		}
	}

	// TODO: broadcast the audio message to all playback clients subscribed to this stream
}

// videoData is the full payload (it has the video headers at the beginning of the payload), for easy forwarding
func (s *Session) onVideoMessage(frameType video.FrameType, codec video.Codec, videoData []byte) {
	if config.Debug {
		switch frameType {
		case video.KeyFrame:
			fmt.Printf("key frame")
		case video.InterFrame:
			fmt.Printf("inter frame")
		}

		switch codec {
		case video.H264:
			fmt.Printf(", h264 (avv)\n")
		case video.H263:
			fmt.Printf(", h263\n")
		}
	}

	// TODO: broadcast the video message to all playback clients subscribed to this stream
}