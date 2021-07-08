/**
 * Sets the active “you can do” slide
 * @param {int} which - index of the slide to activate
 */
function setYouCanDoSlide(which) {
  $("#usageCarousel").carousel(which);
}

/**
 * Ensures that all data teaser cards height match the height of the highest card for uniformity.
 */
function adjustCards() {
  $(".card").animate({ height: "auto" }, function () {
    $(".card").height("auto");
    var maxHeight = Math.max.apply(
      null,
      $(".card")
        .map(function () {
          return $(this).height();
        })
        .get()
    );
    $(".card").height(maxHeight);
  });
}

/**
 * Calls adjustCards on any window resize
 */
$(window)
  .resize(function () {
    adjustCards();
  })
  .resize();

/**
 * Sets up the carousels and loads data into the teaser from the dataItems structure.
 * It also creates cloned copies of the slides and mashes them into new slides that
 * contain three slides each in order to have the three slides showing.
 */
function initIndex() {
  $("#main-carousel").carousel();
  $("#usageCarousel").carousel({
    interval: false,
  });

  $("#dataCarousel").carousel({
    interval: 8000,
    transition: "transform 2s ease, opacity .5s ease-out"
    // transition: "fade-in",
  });

  $('#dataCarousel').on('slide.bs.carousel', function () {
    adjustCards();
  })

  // $("#dataCarousel.carousel .carousel-item").each(function () {
  //   var minPerSlide = 3;
  //   var next = $(this).next();
  //   if (!next.length) {
  //     next = $(this).siblings(":first");
  //   }
  //   next.children(":first-child").clone().appendTo($(this));
  //
  //   for (var i = 0; i < minPerSlide; i++) {
  //     next = next.next();
  //     if (!next.length) {
  //       next = $(this).siblings(":first");
  //     }
  //
  //     next.children(":first-child").clone().appendTo($(this));
  //   }
  // });

  adjustCards();
}

/**
 * Calls initIndex on ready
 *
 * @event index-ready
 */
$(function () {
  initIndex();
});
