package id

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

var (
	totalIDs = new(big.Int).Lsh(big.NewInt(1), 128) // 2^128 total IDs in the DHT ring
	k        int                                    // Number of slices in the DHT ring, must be > 0
	u        int                                    // Number of units per slice in the DHT ring, must be > 0
	sliceSz  *big.Int                               // Size of each slice in the DHT ring
	unitSz   *big.Int                               // Size of each unit in the DHT ring

	// Possible Errors
	// ErrEmptyHexString indicates that the provided hexadecimal string is empty.
	ErrEmptyHexString = errors.New("hex string cannot be empty")
	// ErrInvalidHexLength indicates that the provided hexadecimal string does not have the correct length.
	ErrInvalidHexLength = errors.New("hex string must be exactly 32 characters (16 bytes)")
	// ErrValueTooLarge indicates that the value is outside the 128-bit range.
	ErrValueTooLarge = errors.New("value outside 128‑bit range")
	// ErrInvalidK indicates that K must be greater than 0.
	ErrInvalidK = errors.New("K must be > 0")
	// ErrInvalidU indicates that U must be greater than 0.
	ErrInvalidU = errors.New("K and U must be > 0")
)

// ID represents a unique identifier in the DHT network.
type ID [16]byte

// ---- Initialization of ID Parameters -----

// computeSizes calculates the slice and unit sizes based on the total number of IDs (2^128).
func computeSizes() (sliceSz, unitSz *big.Int) {
	kBig := big.NewInt(int64(k))
	uBig := big.NewInt(int64(u))

	sliceSz = new(big.Int).Div(totalIDs, kBig)
	unitSz = new(big.Int).Div(sliceSz, uBig)
	return sliceSz, unitSz
}

// InitializeIDParameters initializes the global parameters for id package.
func InitializeIDParameters(kVal, uVal int) error {
	if kVal <= 0 {
		return ErrInvalidK
	}
	if uVal <= 0 {
		return ErrInvalidU
	}
	k = kVal
	u = uVal
	sliceSz, unitSz = computeSizes()
	return nil
}

// GetK returns the number of slices in the DHT network.
func GetK() int { return k }

// GetU returns the number of units per slice in the DHT network.
func GetU() int {
	return u
}

// GetSliceSize returns the size of each slice in the DHT network.
func GetSliceSize() *big.Int { return sliceSz }

// GetUnitSize returns the size of each unit in the DHT network.
func GetUnitSize() *big.Int { return unitSz }

// ----- ID creation and conversion -----

// StringToID converts a string to an ID by generating a 16-byte MD5 hash.
func StringToID(a string) ID {
	// Generate a 16-byte MD5 hash that matches our ID type exactly
	hash := md5.Sum([]byte(a))
	return hash
}

// ChunkToID converts a byte slice to an ID. It expects the byte slice to be exactly 16 bytes long.
func ChunkToID(chunk []byte) ID {
	hash := md5.Sum(chunk)
	return hash
}

// GenerateID creates a new ID based on the given address by hashing it with a timestamp salt.
func GenerateID(ip string, port int) ID {
	// Combine the IP and port into a string
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	// insert a timestamp to ensure uniqueness
	addrWithTimestamp := fmt.Sprintf("%s:%d", addr, time.Now().UnixNano())
	// Hash the combined string to create a unique ID
	return StringToID(addrWithTimestamp)
}

// ToHexString converts the ID to a hexadecimal string representation.
func (id ID) ToHexString() string {
	return fmt.Sprintf("%x", id)
}

// FromHexString converts a hexadecimal string representation of an ID back to an ID type.
func FromHexString(hexStr string) (ID, error) {
	if hexStr == "" {
		return ID{}, ErrEmptyHexString
	}
	var id ID
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return id, fmt.Errorf("invalid hex string: %w", err)
	}
	if len(bytes) != len(id) {
		return id, ErrInvalidHexLength
	}
	copy(id[:], bytes)
	return id, nil
}

// idToBig converts a 16‑byte big‑endian ID to *big.Int.
func (id ID) idToBig() *big.Int {
	return big.NewInt(0).SetBytes(id[:])
}

// intToID converts an integer 0<=n<2^128 back to [16]byte (big‑endian).
func bigIntToID(n *big.Int) (ID, error) {
	var id ID
	if n.Sign() < 0 || n.Cmp(totalIDs) >= 0 {
		return ID{}, ErrValueTooLarge
	}
	b := n.Bytes()          // may be shorter than 16 bytes
	copy(id[16-len(b):], b) // left‑pad with zeros
	return id, nil
}

// ---- ID comparison -----

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

// IsOwnedBy return true if myID in (startID, endID] in the ring.
func (id ID) IsOwnedBy(startID, endID ID) bool {
	if startID.LessThan(endID) {
		// normal case: predID < resID < myID
		return id.Equals(endID) || id.LessThan(endID) && startID.LessThan(id)
	} else if startID.Equals(endID) {
		// special case: predID == resID, i.e. the node is alone in the ring
		return id.Equals(startID)
	}
	// wrap‑around
	return id.Equals(endID) || id.LessThan(endID) || startID.LessThan(id)
}

// Next returns the next ID in the ring, wrapping around if necessary.
func (id ID) Next() ID {
	next := id
	// Increment the ID by 1, which is equivalent to adding 1 to the last byte.
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			break
		}
	}
	return next
}

// Prev returns the previous ID in the ring, wrapping around if necessary.
func (id ID) Prev() ID {
	prev := id
	for i := len(prev) - 1; i >= 0; i-- {
		if prev[i] == 0 {
			prev[i] = 0xFF
		} else {
			prev[i]--
			break
		}
	}
	return prev
}

// ---- Manipulation for slice and Unit Calculation ----

// SliceAndUnit returns (slice, unit) in [0,...,K-1]*[0,...,U-1] for the given ID, where K and U are plain int.
// The mapping is uniform except that the last slice/unit may be larger when 2^128 is not divisible by K·U.
func (id ID) SliceAndUnit() (int, int) {
	idInt := id.idToBig()
	sliceIdx := new(big.Int).Div(idInt, sliceSz)
	offset := new(big.Int).Mod(idInt, sliceSz)
	unitIdx := new(big.Int).Div(offset, unitSz)

	return int(sliceIdx.Int64()), int(unitIdx.Int64())
}

// FirstIDOfUnit returns the first ID of the unit that contains id.
func (id ID) FirstIDOfUnit() (ID, error) {
	sliceIdx, unitIdx := id.SliceAndUnit()

	startInt := new(big.Int).Add(
		new(big.Int).Mul(big.NewInt(int64(sliceIdx)), sliceSz),
		new(big.Int).Mul(big.NewInt(int64(unitIdx)), unitSz),
	)
	return bigIntToID(startInt)
}

// FirstIDOfSlice returns the first ID of the slice that contains id.
func (id ID) FirstIDOfSlice() (ID, error) {
	sliceIdx, _ := id.SliceAndUnit()
	startInt := new(big.Int).Mul(big.NewInt(int64(sliceIdx)), sliceSz)
	return bigIntToID(startInt)
}

// SameSlice reports whether id and other fall into the same slice
// using the given K (number of slices). The comparison is 1‑based:
//
//	true  → both IDs map to the identical slice index
//	false → they map to different slices
func (id ID) SameSlice(other ID) bool {
	s1, _ := id.SliceAndUnit()
	s2, _ := other.SliceAndUnit()
	return s1 == s2
}

// SameUnit reports whether id and other fall into the same unit
// when the ID space is partitioned into K slices and U units per slice.
//
// Returns true if both IDs share *both* slice and unit indices.
func (id ID) SameUnit(other ID) bool {
	s1, u1 := id.SliceAndUnit()
	s2, u2 := other.SliceAndUnit()
	return s1 == s2 && u1 == u2
}
