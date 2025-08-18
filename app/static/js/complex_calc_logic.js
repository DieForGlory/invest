document.addEventListener('DOMContentLoaded', function() {
    // --- ОБЩИЕ ПЕРЕМЕННЫЕ И ФУНКЦИИ ---
    const sellIdElement = document.querySelector('h1');
    if (!sellIdElement || !sellIdElement.textContent.includes('ID:')) {
        console.error('Не удалось найти ID объекта на странице.');
        return;
    }
    const sellId = sellIdElement.textContent.split('ID:')[1].trim();

    // Хранилища для данных после успешного расчета
    let installmentCalcData = null;
    let dpInstallmentCalcData = null;

    // Функция для форматирования объекта в строку для URL
    const formatForUrl = (obj) => JSON.stringify(obj);
    // Функция для форматирования чисел
    const formatCurrency = (value) => value.toLocaleString('ru-RU', { maximumFractionDigits: 0 });


    // --- ЛОГИКА ДЛЯ КАЛЬКУЛЯТОРА СТАНДАРТНОЙ РАССРОЧКИ ---
    const installmentForm = document.getElementById('installment-form');
    if (installmentForm) {
        installmentForm.addEventListener('submit', function(e) {
            e.preventDefault();
            const actionUrl = this.dataset.action; // Получаем URL из атрибута формы
            const printBtn = document.getElementById('print-kp-installment');
            printBtn.classList.add('d-none'); // Скрываем кнопку при каждом новом расчете
            installmentCalcData = null; // Сбрасываем данные

            const errorDisplay = document.getElementById('error-display');
            const discountInputs = this.querySelectorAll('.discount-input');
            let additionalDiscounts = {};
            let is_valid = true;

            // Валидация скидок
            discountInputs.forEach(input => {
                input.classList.remove('is-invalid');
                const enteredValue = parseFloat(input.value);
                const maxValue = parseFloat(input.max);
                if (enteredValue > maxValue) {
                    is_valid = false;
                    input.classList.add('is-invalid');
                    errorDisplay.textContent = `Скидка ${input.previousElementSibling.textContent} не может превышать ${input.max}%`;
                    errorDisplay.classList.remove('d-none');
                } else if (enteredValue > 0) {
                    additionalDiscounts[input.id.replace('disc-', '')] = enteredValue / 100.0;
                }
            });

            if (!is_valid) return;
            errorDisplay.classList.add('d-none');
            ['res-price-list', 'res-discount', 'res-contract-value', 'res-monthly-payment'].forEach(id => document.getElementById(id).textContent = '...');

            fetch(actionUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sell_id: sellId,
                    term: document.getElementById('term').value,
                    // ДОБАВЛЯЕМ СТРОКУ
                    start_date: document.getElementById('first_payment_date').value,
                    dp_amount: document.getElementById('dp-amount-standard').value,
                    dp_type: document.getElementById('dp-type-standard').value,
                    additional_discounts: additionalDiscounts
                })
            })
            .then(response => response.json())
            .then(result => {
                if (result.success) {
                    const data = result.data;
                    document.getElementById('res-price-list').textContent = formatCurrency(data.price_list) + ' UZS';
                    document.getElementById('res-discount').textContent = data.calculated_discount.toFixed(2) + ' %';
                    document.getElementById('res-contract-value').textContent = formatCurrency(data.calculated_contract_value) + ' UZS';
                    document.getElementById('res-monthly-payment').textContent = formatCurrency(data.monthly_payment) + ' UZS';

                    // Сохраняем данные и показываем кнопку
                    installmentCalcData = data;
                    printBtn.classList.remove('d-none');
                } else {
                    errorDisplay.textContent = result.error;
                    errorDisplay.classList.remove('d-none');
                }
            })
            .catch(err => {
                console.error("Fetch Error:", err);
                errorDisplay.textContent = 'Ошибка сети. Попробуйте позже.';
                errorDisplay.classList.remove('d-none');
            });
        });
    }


    // --- ЛОГИКА ДЛЯ КАЛЬКУЛЯТОРА РАССРОЧКИ НА ПВ ---
    const dpInstallmentForm = document.getElementById('dp-installment-form');
    if (dpInstallmentForm) {
        dpInstallmentForm.addEventListener('submit', function(e) {
            e.preventDefault();
            const actionUrl = this.dataset.action; // Получаем URL из атрибута формы
            const printBtn = document.getElementById('print-kp-dp-installment');
            printBtn.classList.add('d-none');
            dpInstallmentCalcData = null;

            const errorDisplay = document.getElementById('dp-error-display');
            let is_valid = true;
            let additionalDiscounts = {};

            // Валидация
            this.querySelectorAll('.discount-input-dp').forEach(input => {
                input.classList.remove('is-invalid');
                const enteredValue = parseFloat(input.value);
                const maxValue = parseFloat(input.max);
                if (enteredValue > maxValue) {
                    is_valid = false;
                    input.classList.add('is-invalid');
                    errorDisplay.textContent = `Скидка ${input.previousElementSibling.textContent} не может превышать ${input.max}%`;
                    errorDisplay.classList.remove('d-none');
                } else if (enteredValue > 0) {
                    additionalDiscounts[input.id.replace('dp-disc-', '')] = enteredValue / 100.0;
                }
            });

            if (!is_valid) return;
            errorDisplay.classList.add('d-none');
            ['dp-res-term', 'dp-res-monthly', 'dp-res-mortgage', 'dp-res-contract'].forEach(id => document.getElementById(id).textContent = '...');

            fetch(actionUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sell_id: sellId,
                    term: document.getElementById('dp-term').value,
                    dp_amount: document.getElementById('dp-amount').value,
                    dp_type: document.getElementById('dp-type').value,
                    // ДОБАВЛЯЕМ СТРОКУ
                    start_date: document.getElementById('dp_first_payment_date').value,
                    additional_discounts: additionalDiscounts
                })
            })
            .then(response => response.json())
            .then(result => {
                if (result.success) {
                    const data = result.data;
                    document.getElementById('dp-res-term').textContent = data.term_months + ' мес.';
                    document.getElementById('dp-res-monthly').textContent = formatCurrency(data.monthly_payment_for_dp) + ' UZS';
                    document.getElementById('dp-res-mortgage').textContent = formatCurrency(data.mortgage_body) + ' UZS';
                    document.getElementById('dp-res-contract').textContent = formatCurrency(data.calculated_contract_value) + ' UZS';

                    // Сохраняем данные и показываем кнопку
                    dpInstallmentCalcData = data;
                    printBtn.classList.remove('d-none');
                } else {
                    errorDisplay.textContent = result.error;
                    errorDisplay.classList.remove('d-none');
                }
            })
            .catch(err => {
                console.error("Fetch Error:", err);
                errorDisplay.textContent = 'Ошибка сети. Попробуйте позже.';
                errorDisplay.classList.remove('d-none');
            });
        });
    }

    // --- ОБРАБОТЧИКИ КНОПОК ПЕЧАТИ ---
    const printKpInstallmentBtn = document.getElementById('print-kp-installment');
    if(printKpInstallmentBtn) {
        printKpInstallmentBtn.addEventListener('click', function() {
            if (installmentCalcData) {
                const queryParams = new URLSearchParams({
                    calc_type: 'standard_installment',
                    details: formatForUrl(installmentCalcData)
                });
                window.open(`/reports/commercial-offer/complex/${sellId}?${queryParams.toString()}`, '_blank');
            }
        });
    }

    const printKpDpInstallmentBtn = document.getElementById('print-kp-dp-installment');
    if(printKpDpInstallmentBtn) {
        printKpDpInstallmentBtn.addEventListener('click', function() {
            if (dpInstallmentCalcData) {
                const queryParams = new URLSearchParams({
                    calc_type: 'dp_installment',
                    details: formatForUrl(dpInstallmentCalcData)
                });
                window.open(`/reports/commercial-offer/complex/${sellId}?${queryParams.toString()}`, '_blank');
            }
        });
    }
});