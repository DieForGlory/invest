document.addEventListener('DOMContentLoaded', function() {
    // --- Переключение валют (без изменений) ---
    const currencyToggle = document.getElementById('currencyToggle');
    const currencyLabel = document.getElementById('currencyLabel');
    const usdRate = parseFloat(document.body.dataset.usdRate) || 12650;

    function formatNumber(num) {
        return new Intl.NumberFormat('ru-RU').format(Math.round(num));
    }

    function updateCurrency(isUSD) {
        document.querySelectorAll('.currency-value').forEach(el => {
            const uzsValue = parseFloat(el.dataset.uzsValue);
            if (isNaN(uzsValue)) return;

            if (isUSD) {
                el.textContent = '$ ' + formatNumber(uzsValue / usdRate);
            } else {
                el.textContent = formatNumber(uzsValue);
            }
        });
        if (currencyLabel) {
            currencyLabel.textContent = isUSD ? 'USD' : 'UZS';
        }
        localStorage.setItem('reportCurrency', isUSD ? 'USD' : 'UZS');
    }

    if (currencyToggle) {
        const savedCurrency = localStorage.getItem('reportCurrency');
        if (savedCurrency === 'USD') {
            currencyToggle.checked = true;
        }
        updateCurrency(currencyToggle.checked);

        currencyToggle.addEventListener('change', () => {
            updateCurrency(currencyToggle.checked);
        });
    }

    // --- Логика фильтрации (ОБНОВЛЕНО) ---
    const searchInput = document.querySelector('input[type="text"]'); // Более общий селектор
    const reportRows = document.querySelectorAll('.report-row');
    const hideZeroPlanToggle = document.getElementById('hideZeroPlanToggle'); // НОВОЕ: Находим чекбокс

    function applyFilters() {
        const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';
        const hideZeroPlan = hideZeroPlanToggle ? hideZeroPlanToggle.checked : false; // НОВОЕ: Получаем его состояние

        reportRows.forEach(row => {
            const projectName = row.querySelector('.report-row-title a').textContent.toLowerCase();
            const planUnits = parseInt(row.dataset.planUnits, 10) || 0; // НОВОЕ: Читаем значение плана

            const searchMatch = projectName.includes(searchTerm);
            const planMatch = !hideZeroPlan || planUnits > 0; // НОВОЕ: Проверяем условие для плана

            // НОВОЕ: Условие теперь включает planMatch
            if (searchMatch && planMatch) {
                row.style.display = 'block';
            } else {
                row.style.display = 'none';
            }
        });
    }

    // Привязываем события к фильтрам
    if (searchInput) {
        searchInput.addEventListener('input', applyFilters);
    }
    if (hideZeroPlanToggle) { // НОВОЕ: Привязываем событие к новому фильтру
        hideZeroPlanToggle.addEventListener('change', applyFilters);
    }

    // Применяем фильтры при загрузке страницы
    applyFilters();
});