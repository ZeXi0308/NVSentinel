{{/* An opt-in shared by the requester and the hardware monitor. */}}
{{- define "gpu-health-monitor.gpuRecheckEnabled" -}}
{{- $cfg := (.Values.global | default dict).gpuHealthRecheck | default dict -}}
{{- if hasKey $cfg "enabled" -}}
{{- if not (kindIs "bool" $cfg.enabled) -}}
{{- fail "global.gpuHealthRecheck.enabled must be a boolean" -}}
{{- end -}}
{{- if $cfg.enabled -}}
{{- $_ := required "global.gpuHealthRecheck.nodeSelector is required" $cfg.nodeSelector -}}
true
{{- end -}}
{{- end -}}
{{- end -}}
