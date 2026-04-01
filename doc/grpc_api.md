ListStreamsRequest(limit)
ListStreamsResponse(List[ShortStreamDescription])

GetStreamInfoByIdRequest(id)
GetStreamInfoBySourcePortRequest(sourcePort)
GetStreamInfoResponse(List[FullStreamDescription])

ShortStreamDescription(id, sourcePort, List[sinkPort])
FullStreamDescription(id, createdAt, description, metaData, creator, sourcePort, List[sinkPort], codec, buffer_size)

ProvisionStreamRequest(sourcePort, List[sinkPort], description, metaData)
ProvisionStreamResponse(FullStreamDescription, InvalidPort|Duplicate|LoopDetected|StreamLimitReached)

UpdateStreamRequest(id, description, metaData, List[sinkPort])
UpdateStreamResponse(FullStreamDescription, old: List[sinkPort])
...
SetStreamDescriptionRequest NotAuthorized if not creator
SetStreamMetaDataRequest NotAuthorized
AddStreamMetaDataRequest NotAuthorized
SetStreamsinkPortsRequest(FullStreamDescription, old: List[sinkPort])
AddStreamsinkPortRequest(FullStreamDescription, old: List[sinkPort])

DeprovisionStreamByIdRequest(id)
DeprovisionStreamBysourcePortRequest(sourcePort)
DeprovisionStreamByIdResponse(Option[ShortStreamDescription])
DeprovisionStreamBysourcePortRequest(List[ShortStreamDescription])


SubscribeStreamProvisioningRequest
StreamProvisioned
StreamDeprovisioned



// gets all configured streams
GET /api/v1/streams
{}
->
{
    "streams": [
        "id": "a1",
        "sourcePort": 32100,
        "sinkPorts": [ 32200, 32201, 32203 ]
    ]
    
}

GET /api/v1/streams/${sourcePort}
{}
->
{
    "id": "a1",
    "sourcePort": 32100,
    "sinkPorts": [ 32200, 32201, 32203 ]
}

// adds a new source port
POST /api/v1/streams
{
    "sourcePort": 32100,
    "sinkPorts": [ 32200, 32201, 32203 ]
}
->
{
    "sourcePort": 32100,
    "sinkPorts": [ 32200, 32201, 32203 ]
}

DEL /api/v1/streams