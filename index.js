const moment = require('moment');
const events = require('events');
const fs = require('fs');
const readline = require('readline');

const compareFn = (a, b) => {
  return moment(a.dateTimeString, 'DD-MM-YYYY hh:mm:ss').isBefore(moment(b.dateTimeString, 'DD-MM-YYYY hh:mm:ss')) === true ? -1 : 1;
}

const removeCreditFIFO = (array, negativePoints) => {
  const [first, ...rest] = array;
  if ((first + negativePoints) > 0) {
    return [first + negativePoints].concat(rest);
  } else if ((first + negativePoints) === 0) {
    return rest;
  } else {
    return removeCreditFIFO(rest, first + negativePoints);
  }
}

const removeCreditFIFO_2 = (array, negativePoints) => {
  const [first, ...rest] = array;
  if ((first.credit + negativePoints) > 0) {
    return [{
      ...first,
      credit: first.credit + negativePoints
    }].concat(rest);
  } else if ((first.credit + negativePoints) === 0) {
    return rest;
  } else {
    return removeCreditFIFO_2(rest, first.credit + negativePoints);
  }
}

const processSingleCustomerData = (creditEvents) => {
    const sortedCreditEvents = creditEvents.sort(compareFn);

    console.log(`Single customer credit events - sorted by date`);
    console.log(sortedCreditEvents);

    const positiveCreditEventsBefore2021 = sortedCreditEvents.reduce((prevRes, creditEvent) => {

      // ignore credit events after 2020 i.e. > 2020
      if (moment(creditEvent.dateTimeString, 'DD-MM-YYYY hh:mm:ss').year() > 2020) {
        return prevRes;
      }

      let points = undefined;

      if (prevRes.length === 0) {
        points = parseInt(creditEvent.value, 10);
        return [points];
      } else if (prevRes.length === 1) {
        if (parseInt(creditEvent.value, 10) < 0) {
          return [prevRes[0]+parseInt(creditEvent.value, 10)];
        } else {
          return [prevRes[0], parseInt(creditEvent.value, 10)];
        }
      } else {
        if (parseInt(creditEvent.value, 10) < 0) {
          const last = prevRes.pop();
          if (last + parseInt(creditEvent.value, 10) < 0) {
            const creditToSubtrack = parseInt(creditEvent.value, 10);
            const positiveCreditsSoFar = prevRes.concat([last])
            return removeCreditFIFO(positiveCreditsSoFar, creditToSubtrack);
          }
          return [...prevRes, last + parseInt(creditEvent.value, 10)];
        } else {
          return [...prevRes].concat([parseInt(creditEvent.value, 10)]);
        }
      }
    }, []);

    console.log(`Positive credit events beofre 2021:`);
    console.log(positiveCreditEventsBefore2021);

    const creditsStartingWith2021 = sortedCreditEvents.filter(i => moment(i.dateTimeString, 'DD-MM-YYYY hh:mm:ss').year() >= 2021);

    console.log('Credit events starting with 2021');
    console.log(creditsStartingWith2021);

    const resEvents = creditsStartingWith2021.reduce((prevRes, cur) => {
      if (parseInt(cur.value, 10) > 0) {
        return prevRes.concat([{
          credit: parseInt(cur.value, 10),
          isOldCredit: false
        }]);
      } else {
        const negativeCredit = parseInt(cur.value, 10);
        return removeCreditFIFO_2(prevRes, negativeCredit);
      }
    }, positiveCreditEventsBefore2021.map(credit => ({
      credit,
      isOldCredit: true,
    })));

    console.log(positiveCreditEventsBefore2021);
    console.log(resEvents);

    const res = resEvents.reduce((prev, cur) => {
      if (!cur.isOldCredit) {
        return prev + cur.credit;
      } else {
        return prev;
      }
      
    }, 0);

    if (res < 0) {
      throw new Error('Negative customer score!');
    }

    return res;
};

(async function processLineByLine() {
  try {
    const customersData = {};

    const rl = readline.createInterface({
      input: fs.createReadStream('transactions.csv'),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      // console.log(`Line from file: ${line}`);
      const [customerId, dateTimeString, value] = line.split(',');
      if (!customersData[customerId]) {
        customersData[customerId] = [];
      }
      customersData[customerId].push({
        dateTimeString,
        value,
      });
    });

    await events.once(rl, 'close');

    console.log('Reading file line by line with readline() done.');
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);

    // TODO: create queue for HTTP request call
    // TODO: implement generic queue task tak that will be used to make the HTTP request

    Object.keys(customersData).map(key => {
        const finalScore = processSingleCustomerData(customersData[key]);
        console.log(`Final customer (${key}) score: ${finalScore}`);

        // TODO: add customer score to queue for async handling
    });
  } catch (err) {
    console.error(err);
  }
})();