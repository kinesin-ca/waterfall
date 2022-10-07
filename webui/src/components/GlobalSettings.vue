<script>
export default {
  props: ['refreshSeconds', 'daggydURL'],
  data() {
    return {
      interval: this.refreshSeconds,
      url: this.daggydURL,
    };
  },
  emits: ['update-refresh-interval', 'update-daggyd-url'],
  computed: {
    validRefreshIntervals() {
      return [5, 10, 15, 30, 60, 300, 600];
    },
    isSelected(interval) {
      return (interval === this.refreshSeconds ? 'selected' : 'unselected');
    },
  },
};
</script>

<template>
  <details>
    <summary>Global Settings</summary>
      <label>
        Daggy Base URL
        <input @change="$emit('update-daggyd-url', url)" v-model="url"/>
      </label>
      <label>Refresh Interval (seconds)
        <select @change="$emit('update-refresh-interval', interval)" v-model="interval">
          <option v-for="interval in validRefreshIntervals"
            :key="interval"
            :value="interval"
            >
            {{ interval }} Seconds
            </option>
        </select>
      </label>
  </details>
</template>
