import { computed, reactive } from "vue";

type ToastType = "info" | "success" | "error";

export interface Toast {
  id: number;
  type: ToastType;
  message: string;
}

const state = reactive({
  items: [] as Toast[],
});

let nextId = 1;

export function useToasts() {
  const items = computed(() => state.items);

  function push(message: string, type: ToastType = "info") {
    const toast: Toast = {
      id: nextId++,
      type,
      message,
    };
    state.items.push(toast);
    window.setTimeout(() => {
      state.items = state.items.filter((item) => item.id !== toast.id);
    }, 3800);
  }

  function remove(id: number) {
    state.items = state.items.filter((item) => item.id !== id);
  }

  return { items, push, remove };
}
