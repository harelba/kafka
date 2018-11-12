package kafka.admin.audit

import joptsimple.ValueConverter

object AuditTarget extends Enumeration {
  type AuditTarget = Value
  val StdOut, LogFile, KafkaTopic = Value

  def valueConverter = new ValueConverter[AuditTarget] {
    override def valueType(): Class[_ <: AuditTarget] = classOf[AuditTarget]

    override def convert(value: String): AuditTarget = AuditTarget.withName(value)

    override def valuePattern(): String = AuditTarget.values mkString ","
  }
}
