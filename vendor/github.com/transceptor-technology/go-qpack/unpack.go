// Package qpack provides fast data (de)serializing.
// Maintainer: Jeroen van der Heijden / Transceptor Technology
package qpack

import (
	"encoding/binary"
	"fmt"
)

// QpFlagStringKeysOnly returns each map as map[string]interface{}
const QpFlagStringKeysOnly int = 1

// Unpack return an interface containing deserialized data.
func Unpack(b []byte, flags int) (interface{}, error) {
	var v interface{}
	pos := 0
	err := unpack(&b, &v, &pos, len(b), flags)
	return v, err
}

func unpack(b *[]byte, v *interface{}, pos *int, end, flags int) error {
	if *pos >= end {
		return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
	}
	tp := (*b)[*pos]
	*pos++
	switch tp {
	case '\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08',
		'\x09', '\x0a', '\x0b', '\x0c', '\x0d', '\x0e', '\x0f', '\x10', '\x11',
		'\x12', '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a',
		'\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x20', '\x21', '\x22', '\x23',
		'\x24', '\x25', '\x26', '\x27', '\x28', '\x29', '\x2a', '\x2b', '\x2c',
		'\x2d', '\x2e', '\x2f', '\x30', '\x31', '\x32', '\x33', '\x34', '\x35',
		'\x36', '\x37', '\x38', '\x39', '\x3a', '\x3b', '\x3c', '\x3d', '\x3e',
		'\x3f':
		*v = int(tp)
	case '\x40', '\x41', '\x42', '\x43', '\x44', '\x45', '\x46', '\x47', '\x48',
		'\x49', '\x4a', '\x4b', '\x4c', '\x4d', '\x4e', '\x4f', '\x50', '\x51',
		'\x52', '\x53', '\x54', '\x55', '\x56', '\x57', '\x58', '\x59', '\x5a',
		'\x5b', '\x5c', '\x5d', '\x5e', '\x5f', '\x60', '\x61', '\x62', '\x63',
		'\x64', '\x65', '\x66', '\x67', '\x68', '\x69', '\x6a', '\x6b', '\x6c',
		'\x6d', '\x6e', '\x6f', '\x70', '\x71', '\x72', '\x73', '\x74', '\x75',
		'\x76', '\x77', '\x78', '\x79', '\x7a', '\x7b':
		*v = 63 - int(tp)
	case '\x7c':
		*v = nil // Reserved for Object Hook.
	case '\x7d':
		*v = -1.0
	case '\x7e':
		*v = 0.0
	case '\x7f':
		*v = 1.0
	case '\x80', '\x81', '\x82', '\x83', '\x84', '\x85', '\x86', '\x87', '\x88',
		'\x89', '\x8a', '\x8b', '\x8c', '\x8d', '\x8e', '\x8f', '\x90', '\x91',
		'\x92', '\x93', '\x94', '\x95', '\x96', '\x97', '\x98', '\x99', '\x9a',
		'\x9b', '\x9c', '\x9d', '\x9e', '\x9f', '\xa0', '\xa1', '\xa2', '\xa3',
		'\xa4', '\xa5', '\xa6', '\xa7', '\xa8', '\xa9', '\xaa', '\xab', '\xac',
		'\xad', '\xae', '\xaf', '\xb0', '\xb1', '\xb2', '\xb3', '\xb4', '\xb5',
		'\xb6', '\xb7', '\xb8', '\xb9', '\xba', '\xbb', '\xbc', '\xbd', '\xbe',
		'\xbf', '\xc0', '\xc1', '\xc2', '\xc3', '\xc4', '\xc5', '\xc6', '\xc7',
		'\xc8', '\xc9', '\xca', '\xcb', '\xcc', '\xcd', '\xce', '\xcf', '\xd0',
		'\xd1', '\xd2', '\xd3', '\xd4', '\xd5', '\xd6', '\xd7', '\xd8', '\xd9',
		'\xda', '\xdb', '\xdc', '\xdd', '\xde', '\xdf', '\xe0', '\xe1', '\xe2',
		'\xe3':
		n := int(tp) - 128
		*pos += n
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = string((*b)[*pos-n : *pos])
	case '\xe4':
		if *pos >= end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		n := int((*b)[*pos])
		*pos += n + 1
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = string((*b)[*pos-n : *pos])
	case '\xe5':
		*pos += 2
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		n := int(binary.LittleEndian.Uint16((*b)[*pos-2 : *pos]))
		*pos += n
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = string((*b)[*pos-n : *pos])
	case '\xe6':
		*pos += 4
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		n := int(binary.LittleEndian.Uint32((*b)[*pos-4 : *pos]))
		*pos += n
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = string((*b)[*pos-n : *pos])
	case '\xe7':
		*pos += 8
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		n := int(binary.LittleEndian.Uint16((*b)[*pos-8 : *pos]))
		*pos += n
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = string((*b)[*pos-n : *pos])
	case '\xe8':
		if *pos >= end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = int(int8((*b)[*pos]))
		*pos++
	case '\xe9':
		*pos += 2
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = int(int16FromBytes((*b)[*pos-2 : *pos]))
	case '\xea':
		*pos += 4
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = int(int32FromBytes((*b)[*pos-4 : *pos]))
	case '\xeb':
		*pos += 8
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = int(int64FromBytes((*b)[*pos-8 : *pos]))
	case '\xec':
		*pos += 8
		if *pos > end {
			return fmt.Errorf("Unpack() is missing data at position: %d", *pos)
		}
		*v = float64FromBytes((*b)[*pos-8 : *pos])
	case '\xed', '\xee', '\xef', '\xf0', '\xf1', '\xf2':
		n := int(tp) - 237
		slice := make([]interface{}, n)
		for i := 0; i < n; i++ {
			err := unpack(b, &slice[i], pos, end, flags)
			if err != nil {
				return err
			}
		}
		*v = slice
	case '\xf3', '\xf4', '\xf5', '\xf6', '\xf7', '\xf8':
		n := int(tp) - 243
		if flags&QpFlagStringKeysOnly == 0 {
			m := make(map[interface{}]interface{})
			var key, val interface{}
			for i := 0; i < n; i++ {
				err := unpack(b, &key, pos, end, flags)
				if err != nil {
					return err
				}
				err = unpack(b, &val, pos, end, flags)
				if err != nil {
					return err
				}
				m[key] = val
			}
			*v = m
		} else {
			m := make(map[string]interface{})
			var key, val interface{}
			for i := 0; i < n; i++ {
				err := unpack(b, &key, pos, end, flags)
				if err != nil {
					return err
				}
				err = unpack(b, &val, pos, end, flags)
				if err != nil {
					return err
				}
				keystr, ok := key.(string)
				if !ok {
					return fmt.Errorf(
						"Unpack() got an non string value for key at position: %d",
						*pos)
				}
				m[keystr] = val
			}
			*v = m
		}
	case '\xf9':
		*v = true
	case '\xfa':
		*v = false
	case '\xfb':
		*v = nil
	case '\xfc':
		slice := make([]interface{}, 0)
		var val interface{}
		for *pos < end && (*b)[*pos] != '\xfe' {
			err := unpack(b, &val, pos, end, flags)
			if err != nil {
				return err
			} else if val == '\xff' {
				return fmt.Errorf(
					"Unpack() got an unexpected close map at position: %d",
					*pos)
			}
			slice = append(slice, val)
		}
		(*pos)++
		*v = slice
	case '\xfd':
		if flags&QpFlagStringKeysOnly == 0 {
			m := make(map[interface{}]interface{})
			var key, val interface{}
			for *pos < end && (*b)[*pos] != '\xff' {
				err := unpack(b, &key, pos, end, flags)
				if err != nil {
					return err
				} else if key == '\xfe' {
					return fmt.Errorf(
						"Unpack() got an unexpected close array at position: %d",
						*pos)
				}
				err = unpack(b, &val, pos, end, flags)
				if err != nil {
					return err
				} else if val == '\xfe' {
					return fmt.Errorf(
						"Unpack() got an unexpected close array at position: %d",
						*pos)
				}
				m[key] = val
			}
			*v = m
		} else {
			m := make(map[string]interface{})
			var key, val interface{}
			for *pos < end && (*b)[*pos] != '\xff' {
				err := unpack(b, &key, pos, end, flags)
				if err != nil {
					return err
				} else if key == '\xfe' {
					return fmt.Errorf(
						"Unpack() got an unexpected close array at position: %d",
						*pos)
				}
				err = unpack(b, &val, pos, end, flags)
				if err != nil {
					return err
				} else if val == '\xfe' {
					return fmt.Errorf(
						"Unpack() got an unexpected close array at position: %d",
						*pos)
				}
				keystr, ok := key.(string)
				if !ok {
					return fmt.Errorf(
						"Unpack() got an non string value for key at position: %d",
						*pos)
				}
				m[keystr] = val
			}
			*v = m
		}
		(*pos)++
	case '\xfe', '\xff':
		*v = tp
	default:
		return fmt.Errorf(
			"Unpack() got an unexpected type %d at position: %d", tp, *pos)
	}

	return nil
}
