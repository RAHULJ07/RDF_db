package global;

public enum BPOrder {
  ASCENDING(0),
  DESCENDING(1),
  RANDOM(2);

  private final int order;

  BPOrder(final int order) {
    this.order = order;
  }

  public int getOrder() { return order; }

}
