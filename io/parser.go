package io

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Parser struct {
	reader *bufio.Reader
}

func NewParser(connection net.Conn) *Parser {
	return &Parser{reader: bufio.NewReader(connection)}
}

func (p *Parser) Parse() (Value, error) {
	valType, err := p.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch valType {
	case ARRAY:
		return p.readArray()
	case BULK:
		return p.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(valType))
		return Value{}, nil
	}
}

func (p *Parser) readLine() ([]byte, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("format error: expected \\r\\n")
	}

	// Strip the \r\n
	return line[:len(line)-2], nil
}

func (p *Parser) readInt() (int, error) {
	line, err := p.readLine()
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, err
	}

	return int(i), nil
}

func (p *Parser) readArray() (Value, error) {
	v := Value{valueType: "array"}

	length, err := p.readInt()
	if err != nil {
		return Value{}, err
	}

	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := p.Parse()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}

	return v, nil
}

func (p *Parser) readBulk() (Value, error) {
	v := Value{valueType: "bulk"}

	length, err := p.readInt()
	if err != nil {
		return v, err
	}

	// Null bulk
	if length == -1 {
		v.bulk = ""
		return v, nil
	}

	bulk := make([]byte, length)
	_, err = io.ReadFull(p.reader, bulk)
	if err != nil {
		return v, err
	}
	v.bulk = string(bulk)

	// Strip /r/n
	if b1, _ := p.reader.ReadByte(); b1 != '\r' {
		return v, fmt.Errorf("expected \\r after bulk")
	}
	if b2, _ := p.reader.ReadByte(); b2 != '\n' {
		return v, fmt.Errorf("expected \\n after bulk")
	}

	return v, nil
}
