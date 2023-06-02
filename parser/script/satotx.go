package script

// false if ord 1 1 type 0 content endif
// false false if ord ...
// false if false if ord ...
// false if ord false if ord ...
// false if ord 1 1 false if ord ...
// false if ord 1 1 type false if ord ...
// false if ord 1 1 type 0 false if ord ...
// false if ord 1 1 type 0 content false if ord ...
func ExtractPkScriptForNFT(pkScript []byte) (nft *NFTData, hasNFT bool) {
	length := len(pkScript)
	if length == 0 {
		return
	}

	nft = &NFTData{}
	tags := make(map[string]bool)

	p := uint(0)
	e := uint(length)

	for p < e {
		// check OP_FALSE
		size, data, isPush, isOpcode := GetOpcodeFormScript(pkScript[p:])
		if data == nil {
			break
		}
		p += size // consume OP_CODE, notice: OP_FALSE is a PUSH and a OPCODE
		if !isPush || !isOpcode || size != 1 || data[0] != OP_FALSE {
			// fmt.Println("skip not OP_FALSE")
			// skip if not OP_FALSE
			continue
		}

		// min envlope lenght == 6, OP_IF OP_PUSH_DATA3 'ord' OP_ENDIF
		if p+6 > e {
			break
		}

		// check OP_IF
		size, data, isPush, isOpcode = GetOpcodeFormScript(pkScript[p:])
		if data == nil {
			break
		}
		p += size // consume OP_CODE
		if isPush || !isOpcode || size != 1 || data[0] != OP_IF {
			// fmt.Println("skip not OP_IF")
			// skip if not OP_IF
			continue
		}

		// check OP_PUSH_DATA_3 magic ord
		size, data, isPush, isOpcode = GetOpcodeFormScript(pkScript[p:])
		if data == nil {
			break
		}
		p += size // consume OP_CODE
		if !isPush || len(data) != 3 || data[0] != 'o' || data[1] != 'r' || data[2] != 'd' {
			// skip if not 'ord'
			// fmt.Println("skip not ord", isPush, size, data)
			continue
		}

		offset := p

		// parse nft
		for offset < e {
			// check tag name
			size, data, isPush, isOpcode := GetOpcodeFormScript(pkScript[offset:])
			if data == nil {
				break
			}
			offset += size // consume OP_CODE
			if !isPush {
				if size == 1 && data[0] == OP_ENDIF { // found
					return nft, true
				}
				// check invalid OP_CODE
				break
			}

			// body start
			if size == 1 && data[0] == OP_0 {
				for offset < e {
					size, data, isPush, isOpcode := GetOpcodeFormScript(pkScript[offset:])
					if data == nil {
						break
					}
					offset += size // consume OP_CODE
					if isPush {
						// official need fix minimal push check
						if isOpcode {
							if data[0] == OP_0 { // ? fixme: not sure if ord accept it
								continue
							}
							break
						}

						// append content type data
						nft.ContentBody = append(nft.ContentBody, data...)
					} else {
						if data[0] == OP_ENDIF { // found
							return nft, true
						}
						// check invalid OP_CODE
						break
					}
				}
				break
			}

			// other tag
			if data[0]%2 == 0 {
				break // error: invalid tag
			}

			// official need fix minimal push check
			if isOpcode {
				break
			}

			tagName := string(data)
			if _, ok := tags[tagName]; ok {
				break // error: dup tag
			}
			tags[tagName] = true

			// fixme: minimal pushdata content type
			// if size == 1 && (data[0] == OP_1 || data[0] == OP_DATA_1 ) {
			if len(data) == 1 && data[0] == OP_DATA_1 {
				size, data, isPush, isOpcode := GetOpcodeFormScript(pkScript[offset:])
				if data == nil {
					break
				}
				offset += size // consume OP_CODE
				if isPush {
					// official need fix minimal push check
					if isOpcode {
						if data[0] == OP_0 { // OP_0 is push and opcode, and ord accept it
							continue
						}
						break
					}

					// append content type data
					nft.ContentType = append(nft.ContentType, data...)
				} else {
					if data[0] == OP_ENDIF { // found
						return nft, true
					}
					// check invalid OP_CODE
					break
				}
			} else {
				// skip valid tag
				size, data, isPush, isOpcode := GetOpcodeFormScript(pkScript[offset:])
				if data == nil {
					break
				}
				offset += size // consume OP_CODE
				// skip valid tag
				if !isPush {
					if data[0] == OP_ENDIF { // found
						return nft, true
					}
					// check invalid OP_CODE
					break
				} else {
					// official need fix minimal push check
					if isOpcode {
						if data[0] == OP_0 { // OP_0 is push and opcode, and ord accept it
							continue
						}
						break
					}
				}
			}
		} // for parse nft tags/body

		// restart
		p = offset
	}

	return nil, false
}

func ExtractPkScriptForTxo(pkScript, scriptType []byte) (txo *AddressData) {
	txo = &AddressData{}

	if len(pkScript) == 0 {
		return txo
	}

	if isPubkeyHash(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2PKH
		copy(txo.AddressPkh[:], pkScript[3:23])
		return txo
	}

	if isPayToWitnessPubKeyHash(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2WPKH
		copy(txo.AddressPkh[:], GetHash160(pkScript))
		return txo
	}

	if isPayToWitnessScriptHash(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2WSH
		copy(txo.AddressPkh[:], GetHash160(pkScript))
		return txo
	}

	if isPayToTaproot(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2TR
		copy(txo.AddressPkh[:], GetHash160(pkScript))
		return txo
	}

	if isPayToScriptHash(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2SH
		copy(txo.AddressPkh[:], GetHash160(pkScript))
		return txo
	}

	if isPubkey(scriptType) {
		txo.HasAddress = true
		txo.CodeType = CodeType_P2PK
		copy(txo.AddressPkh[:], GetHash160(pkScript))
		return txo
	}

	// if isMultiSig(scriptType) {
	// 	return pkScript[:]
	// }

	if IsOpreturn(scriptType) {
		return txo
	}

	return txo
}

func GetLockingScriptType(pkScript []byte) (scriptType []byte) {
	length := len(pkScript)
	if length == 0 {
		return
	}
	scriptType = make([]byte, 0)

	lenType := 0
	p := uint(0)
	e := uint(length)

	for p < e && lenType < 32 {
		c := pkScript[p]
		if 0 < c && c < 0x4f {
			cnt, cntsize := SafeDecodeVarIntForScript(pkScript[p:])
			p += cnt + cntsize
			if p > e {
				break
			}
		} else {
			p += 1
		}
		scriptType = append(scriptType, c)
		lenType += 1
	}
	return
}
