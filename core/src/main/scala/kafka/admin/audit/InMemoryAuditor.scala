package kafka.admin.audit

import scala.collection.mutable

class InMemoryAuditor extends Auditor {
  private val audits = mutable.Buffer[(String,String)]()

  override def sendAuditInfo(key:String,s: String) = {
    val t = (key,s)
    audits += t
  }

  def fetchAudits() = audits.toList

  def clearAudits() = audits.clear()
}
