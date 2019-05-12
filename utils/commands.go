package utils

import (
	"github.com/Jeffail/gabs"
)

func CreateRingCommand() string {
	jsonObj := gabs.New()
	jsonObj.Set("create-ring", "do")
	return jsonObj.String()
}

func JoinRingCommand(sponsoringNode string) string {
	jsonObj := gabs.New()
	jsonObj.Set("join-ring", "do")
	jsonObj.Set(sponsoringNode, "sponsoring-node")
	return jsonObj.String()

}
func LeaveRingCommand(mode string) string {
	jsonObj := gabs.New()
	jsonObj.Set("leave-ring", "do")
	jsonObj.Set(mode, "mode")
	return jsonObj.String()
}

func PutCommand(key string, value string, replyTo string) string {
	jsonObj := gabs.New()
	jsonObj.Set("put", "do")
	jsonObj.Set(key, "data", "key")
	jsonObj.Set(value, "data", "value")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj.String()

}

func GetCommand(key string, replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("get", "do")
	jsonObj.Set(key, "data", "key")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
func InitRingFingersCommand() *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("init-ring-fingers", "do")
	return jsonObj
}
func StabilizeRingCommand() *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("stabilize-ring", "do")
	return jsonObj
}
func FixRingFingersCommand() *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("fix-ring-fingers", "do")
	return jsonObj
}
func getRingFingersCommand(replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("get-ring-fingers", "do")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
func RingNotifyCommand(replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("ring-notify", "do")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}

// {"do": "find-ring-successor", "id": id, "reply-to": address}
func FindRingSuccessorCommand(id uint32, replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("find-ring-successor", "do")
	jsonObj.Set(id, "id")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
func FindRingPredecessorCommand(id uint32, replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("find-ring-predecessor", "do")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
func RemoveCommand(key string, replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("remove", "do")
	jsonObj.Set(key, "data", "key")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
func ListItemsCommand(replyTo string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("list-items", "do")
	jsonObj.Set(replyTo, "reply-to")
	return jsonObj
}
