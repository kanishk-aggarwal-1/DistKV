#pragma once

#include "distkv.pb.h"
#include "replication/replication_log.h"

// Conversions between replication ops and their wire form.
namespace kv::replication {

distkv::v1::ReplicationOp opToProto(const Op& op);
Op opFromProto(const distkv::v1::ReplicationOp& proto);

}  // namespace kv::replication
