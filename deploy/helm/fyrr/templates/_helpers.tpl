{{/* Pod security shared by readers and writer: non-root, no extra privileges, read-only root file system. */}}
{{- define "fyrr.podSecurity" -}}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault
{{- end }}

{{- define "fyrr.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end }}
