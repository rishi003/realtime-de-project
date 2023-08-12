import kijiji from 'kijiji-scraper';

export const roomForRentScraper = () => {
  return kijiji
    .search({
      locationId: kijiji.locations.ONTARIO.TORONTO_GTA,
      categoryId: kijiji.categories.REAL_ESTATE.FOR_RENT.LONG_TERM_RENTALS,
      sortType: 'DATE_DESCENDING',
    })
    .then((res) => {
      return res.map((item) => {
        return transformAd(item);
      });
    });
};

export const scrapeAd = (ad) => {
  return kijiji.get(ad.id).then((res) => {
    return transformAd(res);
  });
};

const transformAd = (ad) => {
  return {
    id: ad.id,
    title: ad.title,
    price: ad.attributes.price,
    location: ad.attributes.location,
    description: ad.description,
    url: ad.url,
    date: ad.date,
    furnished: ad.attributes.furnished,
    petsallowed: ad.attributes.petsallowed,
  };
};
