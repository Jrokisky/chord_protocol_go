package utils

import (
	"crypto/sha1"
	"encoding/binary"
	"math/big"
	"math/rand"
	"strconv"
	"os"
	"errors"

	// TODO: remove - debugging
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

// Inclusive
const MinPort = 1025
const MaxPort = 47808
const Localhost = "127.0.0.1"
const ERROR_MSG = "NORESPONSE"

func ComputeId(input string) uint32 {
	// Hash input
	hash := sha1.New()
	hash.Write([]byte(input))
	hashed_in := hash.Sum(nil)

	// Conver input to int
	hash_big := new(big.Int)
	hash_big.SetBytes(hashed_in)

	// Mod chord size
	size_big := new(big.Int)
	size_big.Exp(big.NewInt(2), big.NewInt(32), nil)
	result_big := new(big.Int)
	result_big = hash_big.Mod(hash_big, size_big)

	// Convert to uint32
	return binary.BigEndian.Uint32(result_big.Bytes())
}

func SendMessage(msg string, address string) (string, error) {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()

	Debug("[SendMessage] Sending msg: %s to address: %s\n", msg, address)
	socket.Bind(address)
	socket.Send(msg, 0)

	reply, err := socket.Recv(0)
	if reply == ERROR_MSG || err != nil {
		return "", errors.New("Dropped Message")
	} else {
		return string(reply), nil
	}
}

func GetRandomPort() int {
	return rand.Intn(MaxPort-MinPort) + MinPort
}

func ParseToUInt32(input string) (uint32, error) {
	result64, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		return 0, err
	} else {
		result := uint32(result64)
		return result, nil
	}
}

func Debug(log string, args ...string) {
	typed_args := make([]interface{}, len(args))
	for i, v := range args {
		typed_args[i] = v
	}
	fmt.Fprintf(os.Stderr, log, typed_args...)
}

func IsBetween(start uint32, end uint32, val uint32) bool {
	//---------------------------------------
	// s = start  e = end   v = value
	//     __v___|___e_
	//    /      0     \
	//   s              \
	//  /                \
	if (start > end) && (start < val) && (val > end) {
		return true
	}

	//---------------------------------------
	// s = start  e = end   v = value
	//     ______|_v_e_
	//    /      0     \
	//   s              \
	//  /                \
	if (start > end) && (start > val) && (val < end) {
		return true
	}

	//----------------------------------------
	// s = start  e = end   v = value
	//  \                /
	//   e              /
	//    \___v______s_/
	if (end > start) && (val > start) && (val < end) {
		return true
	}

	return false
}
