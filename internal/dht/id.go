package dht

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"
)

var (
	totalIDs          = new(big.Int).Lsh(big.NewInt(1), 128)
	ErrEmptyHexString = errors.New("hex string cannot be empty")
)

// ID represents a unique identifier in the DHT network.
type ID [16]byte

// GenerateID create a new ID based on the content string (address:port:timestamp)
func GenerateID(addr string) ID {
	// Combine address, port and current timestamp to ensure uniqueness
	timestamp := time.Now().UnixNano()
	content := fmt.Sprintf("%s:%d", addr, timestamp)
	// Generate a 16-byte MD5 hash that matches our ID type exactly
	hash := md5.Sum([]byte(content))
	return hash
}

// LessThan compares two IDs and returns true if the first ID is less than the second ID.
func (id ID) LessThan(b ID) bool {
	for i := 0; i < len(id); i++ {
		if id[i] < b[i] {
			return true
		} else if id[i] > b[i] {
			return false
		}
	}
	return false
}

// Equals compares two IDs and returns true if they are equal.
func (id ID) Equals(b ID) bool {
	for i := 0; i < len(id); i++ {
		if id[i] != b[i] {
			return false
		}
	}
	return true
}

// ToHexString converts the ID to a hexadecimal string representation.
func (id ID) ToHexString() string {
	return fmt.Sprintf("%x", id)
}

// IDFromHexString converts a hexadecimal string representation of an ID back to an ID type.
func IDFromHexString(hexStr string) (ID, error) {
	if hexStr == "" {
		return ID{}, ErrEmptyHexString
	}
	var id ID
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return id, fmt.Errorf("invalid hex string: %w", err)
	}
	if len(bytes) != len(id) {
		return id, fmt.Errorf("invalid length: expected %d bytes but got %d", len(id), len(bytes))
	}
	copy(id[:], bytes)
	return id, nil
}

// ---- ID conversion and manipulation for slice and unit calculations ----

// idToInt converts a 16‑byte big‑endian ID to *big.Int.
func (id ID) idToBig() *big.Int {
	return big.NewInt(0).SetBytes(id[:])
}

// intToID converts an integer 0<=n<2^128 back to [16]byte (big‑endian).
func intToID(n *big.Int) (ID, error) {
	var id ID
	if n.Sign() < 0 || n.Cmp(totalIDs) >= 0 {
		return id, errors.New("value outside 128‑bit range")
	}
	b := n.Bytes()          // may be shorter than 16 bytes
	copy(id[16-len(b):], b) // left‑pad with zeros
	return id, nil
}

// SliceAndUnit returns (slice, unit) ∈ [1..K] × [1..U] for the given ID,
// where K and U are plain int.  The mapping is uniform except that the
// last slice/unit may be larger when 2^128 is not divisible by K·U.
func (id ID) SliceAndUnit(K, U int) (int, int, error) {
	if K <= 0 || U <= 0 {
		return 0, 0, errors.New("K and U must be > 0")
	}

	idInt := id.idToBig()
	kBig := big.NewInt(int64(K))
	uBig := big.NewInt(int64(U))

	sliceSz := new(big.Int).Div(totalIDs, kBig) // IDs per slice
	unitSz := new(big.Int).Div(sliceSz, uBig)   // IDs per unit

	sliceIdx := new(big.Int).Div(idInt, sliceSz) // 0‑based
	offset := new(big.Int).Mod(idInt, sliceSz)
	unitIdx := new(big.Int).Div(offset, unitSz) // 0‑based

	return int(sliceIdx.Int64()) + 1, int(unitIdx.Int64()) + 1, nil
}

// FirstIDOfSlice returns the first ID of the slice that contains id.
func (id ID) FirstIDOfSlice(K int) (ID, error) {
	if K <= 0 {
		return ID{}, errors.New("K must be > 0")
	}
	idInt := id.idToBig()
	kBig := big.NewInt(int64(K))
	sliceSz := new(big.Int).Div(totalIDs, kBig)

	sliceIdx := new(big.Int).Div(idInt, sliceSz) // 0‑based
	startInt := new(big.Int).Mul(sliceIdx, sliceSz)

	return intToID(startInt)
}

// FirstIDOfUnit returns the first ID of the unit that contains id.
func (id ID) FirstIDOfUnit(K, U int) (ID, error) {
	if K <= 0 || U <= 0 {
		return ID{}, errors.New("K and U must be > 0")
	}
	idInt := id.idToBig()
	kBig := big.NewInt(int64(K))
	uBig := big.NewInt(int64(U))

	sliceSz := new(big.Int).Div(totalIDs, kBig)
	unitSz := new(big.Int).Div(sliceSz, uBig)

	sliceIdx := new(big.Int).Div(idInt, sliceSz)
	offset := new(big.Int).Mod(idInt, sliceSz)
	unitIdx := new(big.Int).Div(offset, unitSz)

	startInt := new(big.Int).Add(
		new(big.Int).Mul(sliceIdx, sliceSz), // slice start
		new(big.Int).Mul(unitIdx, unitSz),   // unit offset
	)
	return intToID(startInt)
}

// SameSlice reports whether id and other fall into the same slice
// using the given K (number of slices). The comparison is 1‑based:
//
//	true  → both IDs map to the identical slice index
//	false → they map to different slices
func (id ID) SameSlice(other ID, K int) (bool, error) {
	if K <= 0 {
		return false, errors.New("K must be > 0")
	}

	kBig := big.NewInt(int64(K))
	sliceSz := new(big.Int).Div(totalIDs, kBig)

	// slice index of receiver
	idx1 := new(big.Int).Div(id.idToBig(), sliceSz).Int64()
	// slice index of the parameter
	idx2 := new(big.Int).Div(other.idToBig(), sliceSz).Int64()

	return idx1 == idx2, nil
}

// SameUnit reports whether id and other fall into the same unit
// when the ID space is partitioned into K slices and U units per slice.
//
// Returns true if both IDs share *both* slice and unit indices.
func (id ID) SameUnit(other ID, K, U int) (bool, error) {
	if K <= 0 || U <= 0 {
		return false, errors.New("K and U must be > 0")
	}

	kBig := big.NewInt(int64(K))
	uBig := big.NewInt(int64(U))

	sliceSz := new(big.Int).Div(totalIDs, kBig)
	unitSz := new(big.Int).Div(sliceSz, uBig)

	// --- receiver indices ---
	offset1 := new(big.Int).Mod(id.idToBig(), sliceSz)
	slice1 := new(big.Int).Div(id.idToBig(), sliceSz).Int64()
	unit1 := new(big.Int).Div(offset1, unitSz).Int64()

	// --- parameter indices ---
	offset2 := new(big.Int).Mod(other.idToBig(), sliceSz)
	slice2 := new(big.Int).Div(other.idToBig(), sliceSz).Int64()
	unit2 := new(big.Int).Div(offset2, unitSz).Int64()

	return slice1 == slice2 && unit1 == unit2, nil
}
