package zbl.moonlight.core.timeout;

/**
 * 定时任务
 *  为什么不直接用 Runnable 接口？
 *  主要是为了语义上的清楚。
 */
@FunctionalInterface
public interface TimeoutTask extends Runnable {
}
