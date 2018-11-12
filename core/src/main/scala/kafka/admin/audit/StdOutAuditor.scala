package kafka.admin.audit

class StdOutAuditor extends Auditor {
  def sendAuditInfo(key:String,s: String) = {
    println(s)
  }
}
