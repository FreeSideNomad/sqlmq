package io.freesidenomad.sqlmq.bench;

import io.freesidenomad.sqlmq.bench.BenchHarness.RunResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Renders bench results as bench-summary.md, bench-results.json, bench-results.csv.
 * Stateful: callers feed in samples as they complete and then write whatever subset
 * of formats was requested.
 *
 * Two reporting paths:
 * <ul>
 *   <li>Profile mode — per (profile, storage) bucket, median over runs.</li>
 *   <li>Scan mode — per (scan-label, storage, consumers) bucket, median TPS over runs,
 *       with auto peak detection across the consumer-count axis.</li>
 * </ul>
 */
public final class BenchReporter {

    /**
     * All samples for one bucket. In profile mode the bucket key is (profile, storage); in scan
     * mode it's (scan-label, storage, consumers) — i.e. a different bucket per consumer count.
     */
    public static final class Bucket {
        public final String profile;
        public final String storage;
        public final int consumers;        // 0 in profile mode → ignore; >0 in scan mode → bucket discriminator
        public final List<RunResult> samples = new ArrayList<>();

        Bucket(String profile, String storage, int consumers) {
            this.profile = profile;
            this.storage = storage;
            this.consumers = consumers;
        }

        public RunResult median() {
            if (samples.isEmpty()) return null;
            var sorted = new ArrayList<>(samples);
            sorted.sort(Comparator.comparingDouble(RunResult::tps));
            return sorted.get(sorted.size() / 2);
        }

        public double medianP99Ms() {
            if (samples.isEmpty()) return 0;
            var ps = samples.stream().mapToDouble(RunResult::p99Ms).sorted().toArray();
            return ps[ps.length / 2];
        }
    }

    private final String containerImage;
    private final Instant startedAt;
    /** Profile-mode buckets (key = profile|storage). */
    private final Map<String, Bucket> profileBuckets = new LinkedHashMap<>();
    /** Scan-mode buckets (key = scan-label|storage|consumers). */
    private final Map<String, Bucket> scanBuckets = new LinkedHashMap<>();
    private boolean scanMode = false;
    private String scanLabel = "scan";

    public BenchReporter(String containerImage, Instant startedAt) {
        this.containerImage = containerImage;
        this.startedAt = startedAt;
    }

    /** Switch reporter into scan mode. The {@code scanLabel} appears in markdown headings. */
    public void enableScanMode(String scanLabel) {
        this.scanMode = true;
        this.scanLabel = scanLabel;
    }

    /** Append a single profile-mode run sample. */
    public void add(RunResult sample) {
        var key = sample.profile() + "|" + sample.storage();
        profileBuckets.computeIfAbsent(key, k -> new Bucket(sample.profile(), sample.storage(), 0))
               .samples.add(sample);
    }

    /** Append a single scan-mode run sample (one row per (consumers, storage, run)). */
    public void addScan(RunResult sample) {
        var key = sample.profile() + "|" + sample.storage() + "|" + sample.consumers();
        scanBuckets.computeIfAbsent(key, k -> new Bucket(sample.profile(), sample.storage(), sample.consumers()))
               .samples.add(sample);
    }

    public void writeAll(Path dir, java.util.Set<String> formats, Instant completedAt) throws IOException {
        Files.createDirectories(dir);
        if (formats.contains("md"))   Files.writeString(dir.resolve("bench-summary.md"), markdown(completedAt));
        if (formats.contains("json")) Files.writeString(dir.resolve("bench-results.json"), json(completedAt));
        if (formats.contains("csv"))  Files.writeString(dir.resolve("bench-results.csv"), csv());
    }

    public String markdown(Instant completedAt) {
        var sb = new StringBuilder();
        sb.append("# sqlmq bench results\n\n");
        sb.append("Container: ").append(containerImage == null ? "(external --jdbc-url)" : containerImage).append("\n");
        sb.append("Started:   ").append(startedAt).append("\n");
        sb.append("Completed: ").append(completedAt).append("\n\n");

        if (!scanBuckets.isEmpty()) {
            appendScanSection(sb);
        }
        if (!profileBuckets.isEmpty()) {
            appendProfileSection(sb);
        }
        return sb.toString();
    }

    private void appendProfileSection(StringBuilder sb) {
        sb.append("## Profile runs (TPS as headline)\n\n");
        sb.append("Each row is the median of N runs.\n\n");
        sb.append("| Profile | Storage | runs | TPS (median) | delivered | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        sb.append("|---------|---------|-----:|-------------:|----------:|---------:|---------:|---------:|\n");
        for (var b : profileBuckets.values()) {
            var m = b.median();
            if (m == null) continue;
            sb.append("| ").append(b.profile).append(" | ").append(b.storage).append(" | ")
              .append(b.samples.size()).append(" | ")
              .append(String.format(Locale.ROOT, "%.0f", m.tps())).append(" | ")
              .append(m.delivered()).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p50Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p95Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p99Ms())).append(" |\n");
        }

        // ondisk vs inmemory ratio table — only emit if both variants present for at least one profile.
        var byProfile = new LinkedHashMap<String, Map<String, Bucket>>();
        for (var b : profileBuckets.values()) byProfile.computeIfAbsent(b.profile, k -> new LinkedHashMap<>()).put(b.storage, b);
        boolean anyPair = byProfile.values().stream().anyMatch(m -> m.containsKey("ondisk") && m.containsKey("inmemory"));
        if (anyPair) {
            sb.append("\n### ondisk vs inmemory ratios\n\n");
            sb.append("| Profile | ondisk TPS | inmemory TPS | ratio (im/od) | ondisk p99 | inmemory p99 |\n");
            sb.append("|---------|-----------:|-------------:|--------------:|-----------:|-------------:|\n");
            for (var entry : byProfile.entrySet()) {
                var od = entry.getValue().get("ondisk");
                var im = entry.getValue().get("inmemory");
                if (od == null || im == null) continue;
                var odm = od.median();
                var imm = im.median();
                double ratio = imm.tps() / Math.max(odm.tps(), 0.001);
                sb.append("| ").append(entry.getKey()).append(" | ")
                  .append(String.format(Locale.ROOT, "%.0f", odm.tps())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.0f", imm.tps())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fx", ratio)).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fms", odm.p99Ms())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fms", imm.p99Ms())).append(" |\n");
            }
        }
        sb.append('\n');
    }

    private void appendScanSection(StringBuilder sb) {
        // Sample shape (use any sample to get preload + msgSizeBytes; storages share the workload shape)
        var anySample = scanBuckets.values().iterator().next().samples.get(0);

        sb.append("## Consumer scaling (TPS vs consumer count)\n\n");
        sb.append("Workload: --preload ").append(anySample.preload()).append(" msg")
          .append(", --message-size ").append(anySample.msgSizeBytes()).append(" bytes\n\n");

        // Group buckets by storage, then within each storage by consumer count (sorted).
        var byStorage = new LinkedHashMap<String, Map<Integer, Bucket>>();
        var consumerCounts = new java.util.TreeSet<Integer>();
        var storages = new LinkedHashSet<String>();
        for (var b : scanBuckets.values()) {
            byStorage.computeIfAbsent(b.storage, k -> new java.util.TreeMap<>()).put(b.consumers, b);
            consumerCounts.add(b.consumers);
            storages.add(b.storage);
        }

        // Header
        sb.append("| consumers |");
        for (var stor : storages) sb.append(' ').append(stor).append(" TPS |");
        if (storages.contains("ondisk") && storages.contains("inmemory")) {
            sb.append(" ratio (im/od) |");
        }
        sb.append('\n');
        sb.append("|----------:|");
        for (var ignored : storages) sb.append("-----------:|");
        if (storages.contains("ondisk") && storages.contains("inmemory")) sb.append("--------------:|");
        sb.append('\n');

        // Find peak per storage
        var peakConsumers = new LinkedHashMap<String, Integer>();
        var peakTps = new LinkedHashMap<String, Double>();
        for (var stor : storages) {
            int peakC = -1;
            double peakT = -1;
            for (var entry : byStorage.get(stor).entrySet()) {
                var med = entry.getValue().median();
                if (med != null && med.tps() > peakT) {
                    peakT = med.tps();
                    peakC = entry.getKey();
                }
            }
            peakConsumers.put(stor, peakC);
            peakTps.put(stor, peakT);
        }

        for (int c : consumerCounts) {
            sb.append("| ").append(c).append(" |");
            Double odTps = null, imTps = null;
            boolean anyPeak = false;
            for (var stor : storages) {
                var bk = byStorage.get(stor).get(c);
                if (bk == null) {
                    sb.append("  |");
                } else {
                    var med = bk.median();
                    double t = med == null ? 0 : med.tps();
                    if ("ondisk".equals(stor)) odTps = t;
                    if ("inmemory".equals(stor)) imTps = t;
                    String marker = peakConsumers.get(stor) == c ? "  ← peak" : "";
                    sb.append(' ').append(String.format(Locale.ROOT, "%.0f", t)).append(marker).append(" |");
                    if (peakConsumers.get(stor) == c) anyPeak = true;
                }
            }
            if (storages.contains("ondisk") && storages.contains("inmemory")) {
                if (odTps != null && imTps != null && odTps > 0) {
                    double ratio = imTps / odTps;
                    sb.append(' ').append(String.format(Locale.ROOT, "%.2fx", ratio)).append(" |");
                } else {
                    sb.append("  |");
                }
            }
            sb.append('\n');
            // Suppress unused variable warning while keeping logic obvious
            if (anyPeak) { /* peak markers are inline above */ }
        }

        // Peak summary line
        sb.append("\nPeak: ");
        var first = true;
        for (var stor : storages) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(stor).append(" @ ").append(peakConsumers.get(stor)).append(" consumers (")
              .append(String.format(Locale.ROOT, "%.0f", peakTps.get(stor))).append(" TPS)");
        }
        sb.append(".\n\n");

        // Latency at peak (informational)
        sb.append("### Latency at peak (informational)\n\n");
        sb.append("| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        sb.append("|---------|----------:|---------:|---------:|---------:|\n");
        for (var stor : storages) {
            int peakC = peakConsumers.get(stor);
            if (peakC < 0) continue;
            var bk = byStorage.get(stor).get(peakC);
            if (bk == null) continue;
            var med = bk.median();
            if (med == null) continue;
            sb.append("| ").append(stor).append(" | ").append(peakC).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", med.p50Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", med.p95Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", med.p99Ms())).append(" |\n");
        }
        sb.append('\n');
    }

    public String json(Instant completedAt) {
        // Hand-rolled JSON. The bench output is mechanical and stable, so pulling in
        // Jackson/Gson just for serialization isn't worth the dependency weight in
        // the shaded CLI jar.
        var sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"started_at\": \"").append(startedAt).append("\",\n");
        sb.append("  \"completed_at\": \"").append(completedAt).append("\",\n");
        sb.append("  \"container\": ").append(containerImage == null ? "null" : ("\"" + jsonEscape(containerImage) + "\"")).append(",\n");
        var allBuckets = new ArrayList<Bucket>();
        allBuckets.addAll(profileBuckets.values());
        allBuckets.addAll(scanBuckets.values());
        sb.append("  \"results\": [\n");
        for (int bi = 0; bi < allBuckets.size(); bi++) {
            var b = allBuckets.get(bi);
            sb.append("    {\n");
            sb.append("      \"profile\": \"").append(jsonEscape(b.profile)).append("\",\n");
            sb.append("      \"storage\": \"").append(jsonEscape(b.storage)).append("\",\n");
            if (b.consumers > 0) {
                sb.append("      \"consumers\": ").append(b.consumers).append(",\n");
            }
            sb.append("      \"samples\": [\n");
            for (int si = 0; si < b.samples.size(); si++) {
                var s = b.samples.get(si);
                sb.append("        {")
                  .append("\"run\": ").append(s.runIndex())
                  .append(", \"tps\": ").append(String.format(Locale.ROOT, "%.2f", s.tps()))
                  .append(", \"delivered\": ").append(s.delivered())
                  .append(", \"producers\": ").append(s.producers())
                  .append(", \"consumers\": ").append(s.consumers())
                  .append(", \"preload\": ").append(s.preload())
                  .append(", \"msg_size_bytes\": ").append(s.msgSizeBytes())
                  .append(", \"p50_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p50Ms()))
                  .append(", \"p95_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p95Ms()))
                  .append(", \"p99_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p99Ms()))
                  .append(", \"elapsed_s\": ").append(String.format(Locale.ROOT, "%.3f", s.elapsedSeconds()))
                  .append(", \"producer_errors\": ").append(s.producerErrors())
                  .append(", \"consumer_errors\": ").append(s.consumerErrors())
                  .append("}").append(si < b.samples.size() - 1 ? "," : "").append("\n");
            }
            sb.append("      ],\n");
            var med = b.median();
            if (med != null) {
                sb.append("      \"median_tps\": ").append(String.format(Locale.ROOT, "%.2f", med.tps())).append(",\n");
                sb.append("      \"median_p99_ms\": ").append(String.format(Locale.ROOT, "%.2f", b.medianP99Ms())).append("\n");
            } else {
                sb.append("      \"median_tps\": null,\n");
                sb.append("      \"median_p99_ms\": null\n");
            }
            sb.append("    }").append(bi < allBuckets.size() - 1 ? "," : "").append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String csv() {
        var sb = new StringBuilder();
        sb.append("profile,storage,consumers,producers,run,tps,p50_ms,p95_ms,p99_ms,delivered,elapsed_s,preload,msg_size_bytes,producer_errors,consumer_errors\n");
        for (var b : profileBuckets.values()) {
            for (var s : b.samples) appendCsv(sb, s);
        }
        for (var b : scanBuckets.values()) {
            for (var s : b.samples) appendCsv(sb, s);
        }
        return sb.toString();
    }

    private static void appendCsv(StringBuilder sb, RunResult s) {
        sb.append(s.profile()).append(',')
          .append(s.storage()).append(',')
          .append(s.consumers()).append(',')
          .append(s.producers()).append(',')
          .append(s.runIndex()).append(',')
          .append(String.format(Locale.ROOT, "%.2f", s.tps())).append(',')
          .append(String.format(Locale.ROOT, "%.2f", s.p50Ms())).append(',')
          .append(String.format(Locale.ROOT, "%.2f", s.p95Ms())).append(',')
          .append(String.format(Locale.ROOT, "%.2f", s.p99Ms())).append(',')
          .append(s.delivered()).append(',')
          .append(String.format(Locale.ROOT, "%.3f", s.elapsedSeconds())).append(',')
          .append(s.preload()).append(',')
          .append(s.msgSizeBytes()).append(',')
          .append(s.producerErrors()).append(',')
          .append(s.consumerErrors()).append('\n');
    }

    private static String jsonEscape(String s) {
        var sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else          sb.append(c);
                }
            }
        }
        return sb.toString();
    }
}
