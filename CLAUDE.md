# CLAUDE.md

Bu dosya, bu repoda çalışırken Claude Code'un her oturumda uyması gereken kuralları tanımlar.

## Proje Bağlamı

Event-driven sipariş sisteminde gözlemlenebilirlik (OpenTelemetry) demosu. Mimari, faz planı ve gerekçeler:
- **Yerel plan (commit edilmez):** `docs/plan.md`
- **Commit'lenen spesifikasyon:** `docs/project-domain-spec.md`

Domain/iş mantığı detayı bu dosyaya eklenmez — burada sadece her oturumda geçerli davranışsal kurallar yer alır.

## Solution Yapısı

Tek solution: `OrderTrace.sln`. Yeni proje eklerken mevcut solution'a dahil et.

```
src/
├── OrderTrace.Shared/       # event/contract sınıfları, topic sabitleri
├── OrderTrace.OrderIngest/  # web — Kafka producer
├── OrderTrace.Worker/       # worker — consumer + FraudCheck + DB
└── OrderTrace.FraudCheck/   # web — flaky bağımlılık + chaos
```

## Git Workflow

**Branch modeli:** Sadeleştirilmiş — `main`, `develop`, `feature/*`. `release`/`hotfix` branch'i yok.

- Yeni iş her zaman bir **GitHub Issue**'dan başlar.
- Feature branch `develop`'tan açılır: `feature/<issue-no>-kisa-aciklama` (örn. `feature/2-distributed-tracing`).
- PR'lar **develop**'a açılır. Doğrudan `main`'e commit/PR **yok**.
- `develop` → `main` stabil noktada, ayrı release PR'ıyla.
- PR açıklamasında ilgili issue'ya `Closes #<issue-no>` ile referans ver.
- **Merge stratejisi: Merge commit.** Squash/rebase kullanma.
- **Commit mesaj formatı: Conventional Commits.** Commit dili İngilizce (repo dili İngilizce).
- Asla `main`/`develop` üzerinde doğrudan çalışma; her zaman feature branch.

## Kod Stili (C# / .NET)

- `<summary>` XML doc comment **kullanılmaz**; kendini açıklayan isimlendirme + gerektiğinde tek satır `//` yorum.
- **Nullable reference types açık**; nullable uyarıları göz ardı edilmez.
- **File-scoped namespace** (`namespace X;`).
- Yeni sınıflarda uygunsa **primary constructor**.
- Event/contract sınıfları `OrderTrace.Shared` altında toplanır; servisler arası kopya tanım yok.
- Telemetry yapılandırması **env var odaklı** (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`) tutulur.

## Test

- Kafka/Postgres gerektiren akışlar **Testcontainers** ile yazılır; mock altyapıyla geçiştirilmez.
- Kafka üzerinden trace context taşınımı (producer→consumer aynı `trace_id`) test edilebilir kalır.
- Yeni feature PR'ı, değişikliği kapsayan en az bir test içermeden açılmaz.

## Yapılmaması Gerekenler

- `main`/`develop`'a doğrudan push yok.
- Issue'suz feature branch açma yok.
- Squash/rebase merge yok — sadece merge commit.
- `<summary>` tag'li XML doc comment yok.
- `docs/plan.md` commit etme (yerel plan; `.gitignore`'da).
- Yeni ayrı solution oluşturma — tek `OrderTrace.sln`.
