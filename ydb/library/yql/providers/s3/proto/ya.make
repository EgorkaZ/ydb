OWNER(
    g:yq
    g:yql
)

PROTO_LIBRARY()

SRCS(
    range.proto
    retry_config.proto 
    source.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()


