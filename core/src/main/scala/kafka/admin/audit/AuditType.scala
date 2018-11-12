package kafka.admin.audit

import joptsimple.ValueConverter

object AuditType extends Enumeration {
  type AuditType = Value
  val GroupMetadata, OffsetCommits, OffsetCommitSnapshots, LogEndOffsetSnapshots = Value

  def valueConverter = new ValueConverter[AuditType] {
    override def valueType(): Class[_ <: AuditType] = classOf[AuditType]

    override def convert(value: String): AuditType = AuditType.withName(value)

    override def valuePattern(): String = AuditType.values mkString ","
  }
}
