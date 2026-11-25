package com.demo.lake.common.batch;

/**
 * 一个批作业的展示元数据：名字 + 页面文案 + 展示开关。
 *
 * <p>为什么要放在 common 而不是前端：作业卡片和 {@code --job} 是同一份东西。文案写在
 * Vue 里时，改作业名会出现「进程认、页面不认」——前端按名字查不到文案就把卡片静默过滤掉。
 * 收敛到这里之后，服务端 {@code /api/config} 把元数据随白名单一起下发，前端只负责渲染。</p>
 *
 * <p>record 的不可变语义正好合适，访问器是 fluent 风格（{@code name()} / {@code group()}）；
 * 转 JSON 的字段名由 lake-service 的 {@code JobView} 决定，这里不做序列化适配。</p>
 */
public record JobMeta(String name, String title, String desc, String group,
                      boolean cliOnly, boolean featured) {

    /** 主链路：页面第一组，按顺序跑一遍就是一条数据管道 */
    public static final String PIPELINE = "主链路";
    /** 补充项：Schema 演进 / 仅命令行 / 全链路 */
    public static final String OTHER = "补充项";

    /** 主链路上的一步：卡片平铺，点一下就提交 */
    public static JobMeta pipeline(String name, String title, String desc) {
        return new JobMeta(name, title, desc, PIPELINE, false, false);
    }

    /** 补充项：与主链路分开展示 */
    public static JobMeta other(String name, String title, String desc) {
        return new JobMeta(name, title, desc, OTHER, false, false);
    }

    /** 只能在命令行跑（需要 --sql-file 这种页面给不出的参数）：按钮置灰并说明原因 */
    public JobMeta withCliOnly() {
        return new JobMeta(name, title, desc, group, true, featured);
    }

    /** 视觉上强调：一键跑完整条链路的入口 */
    public JobMeta withFeatured() {
        return new JobMeta(name, title, desc, group, cliOnly, true);
    }
}
