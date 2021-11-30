package byteplus.sdk.core.metrics;

import lombok.Getter;

public class Item<T> {
    @Getter
    private final String tags;
    @Getter
    private final T value;

    private final long expireTime;

    public Item(String tags, T value) {
        this(tags, value, Constant.DEFAULT_EXPIRE_TIME_MS);
    }

    public Item(String tags, T value, long expireTime) {
        expireTime = expireTime <= 0 ? Constant.DEFAULT_EXPIRE_TIME_MS : expireTime;
        this.tags = tags;
        this.value = value;
        this.expireTime = System.currentTimeMillis() + expireTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > this.expireTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (obj instanceof Item) {
            Item item = (Item) obj;
            return this.getTags().equals(item.getTags());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return tags.hashCode();
    }

    @Override
    public String toString() {
        return "Item{" +
                "tags='" + tags + '\'' +
                ", value=" + value +
                '}';
    }
}
