package io.freesidenomad.sqlmq.bench;

import io.freesidenomad.sqlmq.bench.BenchHarness.RunResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Renders bench results as bench-summary.md, bench-results.json, bench-results.csv.
 * Stateful: callers feed in samples as they complete and then write whatever subset
 * of formats was requested.
 */
public final class BenchReporter {

    /** All samples for one (profile, storage) combination. */
    public static final class Bucket {
        public final String profile;
        public final String storage;
        public final List<RunResult> samples = new ArrayList<>();

        Bucket(String profile, String storage) {
            this.profile = profile;
            this.storage = storage;
        }

        public RunResult median() {
            if (samples.isEmpty()) return null;
            var sorted = new ArrayList<>(samples);
            sorted.sort(Comparator.comparingDouble(RunResult::msgsPerSec));
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
    private final Map<String, Bucket> buckets = new LinkedHashMap<>();

    public BenchReporter(String containerImage, Instant startedAt) {
        this.containerImage = containerImage;
        this.startedAt = startedAt;
    }

    /** Append a single run sample. Order of additions matters for table layout. */
    public void add(RunResult sample) {
        var key = sample.profile() + "|" + sample.storage();
        buckets.computeIfAbsent(key, k -> new Bucket(sample.profile(), sample.storage()))
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
        sb.append("Each row is the median of N runs.\n\n");
        sb.append("| Profile | Storage | runs | msgs/sec (median) | p50 (ms) | p95 (ms) | p99 (ms) | delivered |\n");
        sb.append("|---------|---------|-----:|------------------:|---------:|---------:|---------:|----------:|\n");
        for (var b : buckets.values()) {
            var m = b.median();
            if (m == null) continue;
            sb.append("| ").append(b.profile).append(" | ").append(b.storage).append(" | ")
              .append(b.samples.size()).append(" | ")
              .append(String.format(Locale.ROOT, "%.0f", m.msgsPerSec())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p50Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p95Ms())).append(" | ")
              .append(String.format(Locale.ROOT, "%.2f", m.p99Ms())).append(" | ")
              .append(m.delivered()).append(" |\n");
        }

        // ondisk vs inmemory ratio table — only emit if both variants present for at least one profile.
        var byProfile = new LinkedHashMap<String, Map<String, Bucket>>();
        for (var b : buckets.values()) byProfile.computeIfAbsent(b.profile, k -> new LinkedHashMap<>()).put(b.storage, b);
        boolean anyPair = byProfile.values().stream().anyMatch(m -> m.containsKey("ondisk") && m.containsKey("inmemory"));
        if (anyPair) {
            sb.append("\n## ondisk vs inmemory ratios\n\n");
            sb.append("| Profile | ondisk msgs/sec | inmemory msgs/sec | ratio | ondisk p99 | inmemory p99 |\n");
            sb.append("|---------|----------------:|------------------:|------:|-----------:|-------------:|\n");
            for (var entry : byProfile.entrySet()) {
                var od = entry.getValue().get("ondisk");
                var im = entry.getValue().get("inmemory");
                if (od == null || im == null) continue;
                var odm = od.median();
                var imm = im.median();
                double ratio = imm.msgsPerSec() / Math.max(odm.msgsPerSec(), 0.001);
                sb.append("| ").append(entry.getKey()).append(" | ")
                  .append(String.format(Locale.ROOT, "%.0f", odm.msgsPerSec())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.0f", imm.msgsPerSec())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fx", ratio)).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fms", odm.p99Ms())).append(" | ")
                  .append(String.format(Locale.ROOT, "%.2fms", imm.p99Ms())).append(" |\n");
            }
        }
        return sb.toString();
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
        sb.append("  \"results\": [\n");
        var buckList = new ArrayList<>(buckets.values());
        for (int bi = 0; bi < buckList.size(); bi++) {
            var b = buckList.get(bi);
            sb.append("    {\n");
            sb.append("      \"profile\": \"").append(jsonEscape(b.profile)).append("\",\n");
            sb.append("      \"storage\": \"").append(jsonEscape(b.storage)).append("\",\n");
            sb.append("      \"samples\": [\n");
            for (int si = 0; si < b.samples.size(); si++) {
                var s = b.samples.get(si);
                sb.append("        {")
                  .append("\"run\": ").append(s.runIndex())
                  .append(", \"msgs_per_sec\": ").append(String.format(Locale.ROOT, "%.2f", s.msgsPerSec()))
                  .append(", \"p50_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p50Ms()))
                  .append(", \"p95_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p95Ms()))
                  .append(", \"p99_ms\": ").append(String.format(Locale.ROOT, "%.2f", s.p99Ms()))
                  .append(", \"delivered\": ").append(s.delivered())
                  .append(", \"elapsed_s\": ").append(String.format(Locale.ROOT, "%.3f", s.elapsedSeconds()))
                  .append(", \"producer_errors\": ").append(s.producerErrors())
                  .append(", \"consumer_errors\": ").append(s.consumerErrors())
                  .append("}").append(si < b.samples.size() - 1 ? "," : "").append("\n");
            }
            sb.append("      ],\n");
            var med = b.median();
            if (med != null) {
                sb.append("      \"median_msgs_per_sec\": ").append(String.format(Locale.ROOT, "%.2f", med.msgsPerSec())).append(",\n");
                sb.append("      \"median_p99_ms\": ").append(String.format(Locale.ROOT, "%.2f", b.medianP99Ms())).append("\n");
            } else {
                sb.append("      \"median_msgs_per_sec\": null,\n");
                sb.append("      \"median_p99_ms\": null\n");
            }
            sb.append("    }").append(bi < buckList.size() - 1 ? "," : "").append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String csv() {
        var sb = new StringBuilder();
        sb.append("profile,storage,run,msgs_per_sec,p50_ms,p95_ms,p99_ms,delivered,elapsed_s,producer_errors,consumer_errors\n");
        for (var b : buckets.values()) {
            for (var s : b.samples) {
                sb.append(s.profile()).append(',')
                  .append(s.storage()).append(',')
                  .append(s.runIndex()).append(',')
                  .append(String.format(Locale.ROOT, "%.2f", s.msgsPerSec())).append(',')
                  .append(String.format(Locale.ROOT, "%.2f", s.p50Ms())).append(',')
                  .append(String.format(Locale.ROOT, "%.2f", s.p95Ms())).append(',')
                  .append(String.format(Locale.ROOT, "%.2f", s.p99Ms())).append(',')
                  .append(s.delivered()).append(',')
                  .append(String.format(Locale.ROOT, "%.3f", s.elapsedSeconds())).append(',')
                  .append(s.producerErrors()).append(',')
                  .append(s.consumerErrors()).append('\n');
            }
        }
        return sb.toString();
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
