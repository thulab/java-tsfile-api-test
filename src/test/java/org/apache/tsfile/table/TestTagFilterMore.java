package org.apache.tsfile.table;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.factory.TagFilterBuilder;
import org.apache.tsfile.util.TestDataUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.regex.PatternSyntaxException;

/**
 * 补充 {@link TagFilterBuilder} 现有测试未覆盖的场景：非法正则、null pattern、value 传非 String、
 * betweenAnd 反序区间、tag 列大小写不敏感。
 */
public class TestTagFilterMore {

  private final TableSchema schema = TestDataUtils.buildTableSchema();
  private final TagFilterBuilder builder = new TagFilterBuilder(schema);
  private static final String TAG = "Tag1";

  /** 非法正则字符串应抛 PatternSyntaxException（Pattern.compile 触发）。 */
  @Test
  public void testInvalidRegexThrows() {
    Assert.expectThrows(PatternSyntaxException.class, () -> builder.regExp(TAG, "["));
    Assert.expectThrows(PatternSyntaxException.class, () -> builder.notRegExp(TAG, "(unclosed"));
  }

  /** regExp / notRegExp 的 null pattern 应抛 NullPointerException（Pattern.compile(null)）。 */
  @Test
  public void testNullRegexPatternThrows() {
    Assert.expectThrows(NullPointerException.class, () -> builder.regExp(TAG, null));
    Assert.expectThrows(NullPointerException.class, () -> builder.notRegExp(TAG, null));
  }

  /** like / notLike 的 null pattern 应抛 NullPointerException。 */
  @Test
  public void testNullLikePatternThrows() {
    Assert.expectThrows(NullPointerException.class, () -> builder.like(TAG, null));
    Assert.expectThrows(NullPointerException.class, () -> builder.notLike(TAG, null));
  }

  /**
   * value 传非 String 类型：TagFilterBuilder 将 value 强转为 String。传入 Integer 会在构造 filter 时
   * 抛 ClassCastException（Object → String 的强转失败）。
   */
  @Test
  public void testNonStringValueThrows() {
    Assert.expectThrows(ClassCastException.class, () -> builder.eq(TAG, 123));
  }

  /**
   * betweenAnd 反序区间（min > max）不应抛异常，只是构造出一个不会命中任何值的 filter；
   * 此处仅验证构造成功、返回非 null。
   */
  @Test
  public void testBetweenAndReversedRange() {
    Assert.assertNotNull(builder.betweenAnd(TAG, "Tag1_Value_9", "Tag1_Value_1"));
  }

  /** tag 列名大小写不敏感：用大写列名构造 filter 应成功（列名内部小写化）。 */
  @Test
  public void testTagColumnCaseInsensitive() {
    Assert.assertNotNull(builder.eq("TAG1", "Tag1_Value_1"), "大写 tag 列名应可用");
    Assert.assertNotNull(builder.eq("tag1", "Tag1_Value_1"), "小写 tag 列名应可用");
  }
}
