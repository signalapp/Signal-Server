const storageKey = 'theme-preference';

const onClick = () => {
    // flip current value
    theme.value = theme.value === 'light'
        ? 'dark'
        : 'light';

    setPreference();
}

const getColorPreference = () => {
    if (localStorage.getItem(storageKey))
        return localStorage.getItem(storageKey);
    else
        return window.matchMedia('(prefers-color-scheme: dark)').matches
            ? 'dark'
            : 'light';
}

const setPreference = () => {
    localStorage.setItem(storageKey, theme.value);
    reflectPreference();
}

const reflectPreference = () => {
    document.documentElement.className =
        theme.value === 'dark' ? 'dark' : '';

    document.documentElement
        .setAttribute('data-theme', theme.value);
}

const theme = {
    value: getColorPreference(),
}

window.addEventListener(
    "load",
    () => {
        document
            .querySelector('#theme-toggle')
            .addEventListener('click', onClick);
    });

// sync with system changes
window
    .matchMedia('(prefers-color-scheme: dark)')
    .addEventListener('change', ({ matches: isDark }) => {
        theme.value = isDark ? 'dark' : 'light';
        setPreference();
    });

reflectPreference();
