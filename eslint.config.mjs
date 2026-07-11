// ESLint 9.x flat config — js/ 브라우저 ES 모듈 + tests/js/ node 테스트 대상 최소 린트.
// 스타일 규칙 없이 실수 탐지(no-undef, no-unused-vars)만 검사한다.
// 실행 예: npx --yes eslint@9 --config eslint.config.mjs "js/**/*.js" "tests/js/**/*.mjs" "tests/js/**/*.js"

export default [
  {
    // 생성 데이터/외부 산출물은 린트 대상에서 제외
    ignores: ['data.js', 'node_modules/**', 'data/**', 'docs/**'],
  },
  {
    files: ['js/**/*.js'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: {
        // 브라우저 전역 (globals 패키지 없이 직접 나열; readonly)
        document: 'readonly',
        window: 'readonly',
        navigator: 'readonly',
        location: 'readonly',
        history: 'readonly',
        screen: 'readonly',
        localStorage: 'readonly',
        sessionStorage: 'readonly',
        fetch: 'readonly',
        URLSearchParams: 'readonly',
        WebSocket: 'readonly',
        EventSource: 'readonly',
        DOMParser: 'readonly',
        Intl: 'readonly',
        console: 'readonly',
        requestAnimationFrame: 'readonly',
        cancelAnimationFrame: 'readonly',
        requestIdleCallback: 'readonly',
        cancelIdleCallback: 'readonly',
        AbortController: 'readonly',
        AbortSignal: 'readonly',
        Blob: 'readonly',
        File: 'readonly',
        FileReader: 'readonly',
        FormData: 'readonly',
        Headers: 'readonly',
        Request: 'readonly',
        Response: 'readonly',
        URL: 'readonly',
        CustomEvent: 'readonly',
        Event: 'readonly',
        EventTarget: 'readonly',
        Element: 'readonly',
        HTMLElement: 'readonly',
        Node: 'readonly',
        NodeList: 'readonly',
        MutationObserver: 'readonly',
        ResizeObserver: 'readonly',
        IntersectionObserver: 'readonly',
        performance: 'readonly',
        crypto: 'readonly',
        atob: 'readonly',
        btoa: 'readonly',
        TextEncoder: 'readonly',
        TextDecoder: 'readonly',
        structuredClone: 'readonly',
        queueMicrotask: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
        setInterval: 'readonly',
        clearInterval: 'readonly',
        getComputedStyle: 'readonly',
        matchMedia: 'readonly',
        devicePixelRatio: 'readonly',
        innerWidth: 'readonly',
        innerHeight: 'readonly',
        alert: 'readonly',
        confirm: 'readonly',
        prompt: 'readonly',
        Image: 'readonly',
        Worker: 'readonly',
        BroadcastChannel: 'readonly',
        Notification: 'readonly',
      },
    },
    rules: {
      // 최소 규칙셋: 정의되지 않은 식별자/미사용 변수만 오류 처리.
      // (다른 작업과의 병렬 수정 충돌을 피하기 위해 스타일 규칙은 두지 않음)
      'no-undef': 'error',
      'no-unused-vars': ['error', { args: 'none', caughtErrors: 'none' }],
    },
  },
  {
    // node 내장 test runner용 테스트 (node --test tests/js/)
    files: ['tests/js/**/*.js', 'tests/js/**/*.mjs'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: {
        console: 'readonly',
      },
    },
    rules: {
      'no-undef': 'error',
      'no-unused-vars': ['error', { args: 'none', caughtErrors: 'none' }],
    },
  },
];
