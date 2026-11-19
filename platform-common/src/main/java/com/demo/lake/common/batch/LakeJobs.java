package com.demo.lake.common.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 批作业名称的唯一来源。
 *
 * <p>作业名是一条跨模块契约：BatchApp 拿它做注册键、{@code --job} 拿它做取值、
 * lake-service 拿它做可触发白名单。名字散落在各模块时，改一次就要全仓搜索，漏一处
 * 就是页面点下去报「未知 job」，所以统一收敛到这里。</p>
 *
 * <p>这里只有名字，没有实现：common 不依赖 Spark。新增作业时先在本类加常量并串进
 * {@link #NAMES}，再去 spark-batch 注册实现、按需调整服务端白名单。</p>
 */
public final class LakeJobs {

    public static final String INIT = "init";
    public static final String INGEST = "ingest";
    public static final String TRANSFORM = "transform";
    public static final String EVOLVE = "evolve";
    public static final String MAINTAIN = "maintain";
    public static final String SHOW = "show";
    public static final String VERIFY_SQL = "verify-sql";

    /** 跑完整条链路的快捷方式：不是作业实现，但同样是 --job 的合法取值 */
    public static final String ALL = "all";

    /** 已注册的作业名，顺序即 --help 与页面里的展示顺序 */
    public static final List<String> NAMES = List.of(
            INIT, INGEST, TRANSFORM, EVOLVE, MAINTAIN, SHOW, VERIFY_SQL);

    /** 允许作为 --job 取值的全部名字（{@link #NAMES} + {@link #ALL}），服务端白名单的默认值 */
    public static final List<String> CHOICES = choices();

    /**
     * 作业的展示元数据，顺序与 {@link #CHOICES} 一致。
     *
     * <p>页面上的标题/描述/是否只能跑命令行都在这里，改作业名时文案跟着走，
     * 不会出现前端按旧名字查不到、把卡片默默丢掉的情况。</p>
     */
    public static final List<JobMeta> METAS = List.of(
            JobMeta.pipeline(INIT, "建库建表",
                    "建业务库、三层 Iceberg 表（bronze/silver/gold）与 ops.batch_runs；"
                            + "全部 IF NOT EXISTS，幂等可重跑，不改已有表结构"),
            JobMeta.pipeline(INGEST, "数据入湖",
                    "按 --days(默认 7) × --orders-per-day(默认 2000) 生成 Parquet 落地文件到 data/raw/orders，"
                            + "再写入 bronze；内含约 3% 重复单与 10% 迟到数据"),
            JobMeta.pipeline(TRANSFORM, "加工汇总",
                    "bronze 近 N 天按 order_id 取最新事件 MERGE 进 silver（只更新变化的行），"
                            + "再 INSERT OVERWRITE 重算 gold 的 GMV / 退款 / 净收入"),
            JobMeta.other(EVOLVE, "Schema 演进",
                    "给 silver 增加 channel 列（APP/MINI_APP/WEB）并 UPDATE 回填；"
                            + "ADD COLUMNS 只改元数据，列已存在时自动跳过"),
            JobMeta.pipeline(MAINTAIN, "表维护",
                    "对三张业务表合并小文件、重写 manifest、清理过期快照"
                            + "（--retain-last 默认保留 5 个）；建议按天调度"),
            JobMeta.pipeline(SHOW, "数据概览",
                    "只读巡检：三层行数、gold 最近快照、bronze 提交历史与分区布局，"
                            + "外加 silver/gold 数据抽样"),
            JobMeta.other(VERIFY_SQL, "SQL 校验",
                    "用 Spark 执行 --sql-file 指定的 ad-hoc 脚本（默认 01_daily_revenue.sql）；"
                            + "跳过 @trino-only 语句，其余做最小方言适配，有失败即作业失败").withCliOnly(),
            JobMeta.other(ALL, "全链路",
                    "按 init → ingest → transform → maintain → show 顺序跑完整链路"
                            + "（加 --evolve 会插入 evolve），全程复用同一个 SparkSession").withFeatured());

    /**
     * 白名单 → 页面要渲染的卡片。
     *
     * <p>按 {@link #METAS} 的顺序返回，与配置里写的顺序无关：页面顺序是产品决定，
     * 不该因为有人把 yml 里的 allowed-jobs 调了个位置就变。白名单里不认识的作业名直接忽略。</p>
     */
    public static List<JobMeta> visible(List<String> allowed) {
        if (allowed == null || allowed.isEmpty()) {
            return List.of();
        }
        List<JobMeta> out = new ArrayList<>(allowed.size());
        for (JobMeta meta : METAS) {
            if (allowed.contains(meta.name())) {
                out.add(meta);
            }
        }
        return out;
    }

    private static List<String> choices() {
        List<String> names = new ArrayList<>(NAMES);
        names.add(ALL);
        return Collections.unmodifiableList(names);
    }

    private LakeJobs() {
    }
}
