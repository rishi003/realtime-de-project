const kijiji = require('kijiji-scraper');

kijiji
  .search({
    locationId: kijiji.locations.ONTARIO.TORONTO_GTA,
    categoryId: kijiji.categories.REAL_ESTATE.FOR_RENT.STORAGE_AND_PARKING_FOR_RENT,
    sortByName: 'priceAsc',
    priceType: 'SPECIFIED_AMOUNT',
  })
  .then((res) => {
    res.map((item) => {
      console.log(item.toString());
    });
  });
