package kafka.admin.audit

import org.slf4j.LoggerFactory

class Log4JAuditor extends Auditor {
  private lazy val auditLogger = LoggerFactory.getLogger("consumer_groups_audit_logger")

  def sendAuditInfo(key:String,s: String) = {
    auditLogger.info(s)
  }
}
