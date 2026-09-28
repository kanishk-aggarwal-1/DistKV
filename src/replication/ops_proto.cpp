#include "replication/ops_proto.h"

namespace kv::replication {

distkv::v1::ReplicationOp opToProto(const Op& op) {
  distkv::v1::ReplicationOp proto;
  proto.set_seq(op.seq);
  switch (op.type) {
    case Op::Type::kPut:
      proto.mutable_put()->set_key(op.key);
      proto.mutable_put()->set_value(op.value);
      break;
    case Op::Type::kDelete:
      proto.set_del(op.key);
      break;
    case Op::Type::kSlotSnapshot: {
      auto* snapshot = proto.mutable_snapshot();
      snapshot->set_slot(op.slot);
      for (const KeyValue& kv : op.entries) {
        auto* entry = snapshot->add_entries();
        entry->set_key(kv.key);
        entry->set_value(kv.value);
      }
      break;
    }
    case Op::Type::kSlotMigrating:
      proto.mutable_migrating()->set_slot(op.slot);
      proto.mutable_migrating()->set_target_client_addr(op.peer_addr);
      break;
    case Op::Type::kSlotImporting:
      proto.mutable_importing()->set_slot(op.slot);
      proto.mutable_importing()->set_migration_id(op.migration_id);
      break;
  }
  return proto;
}

Op opFromProto(const distkv::v1::ReplicationOp& proto) {
  Op op;
  op.seq = proto.seq();
  switch (proto.op_case()) {
    case distkv::v1::ReplicationOp::kPut:
      op.type = Op::Type::kPut;
      op.key = proto.put().key();
      op.value = proto.put().value();
      break;
    case distkv::v1::ReplicationOp::kDel:
      op.type = Op::Type::kDelete;
      op.key = proto.del();
      break;
    case distkv::v1::ReplicationOp::kSnapshot:
      op.type = Op::Type::kSlotSnapshot;
      op.slot = static_cast<uint16_t>(proto.snapshot().slot());
      for (const auto& entry : proto.snapshot().entries()) {
        op.entries.push_back({entry.key(), entry.value()});
      }
      break;
    case distkv::v1::ReplicationOp::kMigrating:
      op.type = Op::Type::kSlotMigrating;
      op.slot = static_cast<uint16_t>(proto.migrating().slot());
      op.peer_addr = proto.migrating().target_client_addr();
      break;
    case distkv::v1::ReplicationOp::kImporting:
      op.type = Op::Type::kSlotImporting;
      op.slot = static_cast<uint16_t>(proto.importing().slot());
      op.migration_id = proto.importing().migration_id();
      break;
    case distkv::v1::ReplicationOp::OP_NOT_SET:
      break;
  }
  return op;
}

}  // namespace kv::replication
