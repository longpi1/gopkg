package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	"golang.org/x/crypto/bcrypt"
)

func Password2Hash(password string) (string, error) {
	passwordBytes := []byte(password)
	hashedPassword, err := bcrypt.GenerateFromPassword(passwordBytes, bcrypt.DefaultCost)
	return string(hashedPassword), err
}

func ValidatePasswordAndHash(password string, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func SHA256(src string, salt string) string {
	h := sha256.New()
	h.Write([]byte(src + salt))
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)

	return s
}

// PasswordEncrypt encrypt password
func PasswordEncrypt(pwd string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.MinCost)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}

func Base64Decode(pwd string) (string, error) {
	bytes, err := base64.StdEncoding.DecodeString(pwd)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}

func Base64Encode(pwd string) string {
	return base64.StdEncoding.EncodeToString([]byte(pwd))
}

func MD5(str string) string {
	// #nosec
	data := md5.Sum([]byte(str))
	return hex.EncodeToString(data[:])[8:24]
}
