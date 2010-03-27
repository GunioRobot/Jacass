package me.arin.jacass;

/**
 * Indexing info
 */
class IndexInfo {
    private boolean isRequired;
    private boolean isUnique;

    IndexInfo(boolean required, boolean unique) {
        isRequired = required;
        isUnique = unique;
    }

    public boolean isRequired() {
        return isRequired;
    }

    public boolean isUnique() {
        return isUnique;
    }
}
