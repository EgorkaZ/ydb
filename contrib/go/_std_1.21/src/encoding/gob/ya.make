GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ENDIF()
END()
